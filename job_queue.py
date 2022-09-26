from contextlib import contextmanager, suppress
import logging
import math
import os
import re
import shlex
import subprocess
import sys
import weakref
import abc
import tempfile
import copy
import warnings

import dask

from dask.utils import format_bytes, parse_bytes, tmpfile

from distributed.core import Status
from distributed.deploy.spec import ProcessInterface, SpecCluster
from distributed.deploy.local import nprocesses_nthreads
from distributed.scheduler import Scheduler
from distributed.security import Security


class MultiSpecCluster(SpecCluster):
    def __init__(
        self,
        workers=None,
        scheduler=None,
        asynchronous=False,
        loop=None,
        security=None,
        silence_logs=False,
        name=None,
        shutdown_on_close=True,
        scheduler_sync_interval=1,
        new_worker_spec_by_type=None,
    ):
        super().__init__(
            workers=workers,
            scheduler=scheduler,
            worker=None,
            loop=loop,
            security=security,
            silence_logs=silence_logs,
            asynchronous=asynchronous,
            name=name,
        )

        # Multiple types of worker can be spun up at run time,
        # but every type of worker that will be spun up must be
        # specified ahead of run time
        self.new_worker_spec_by_type = copy.copy(new_worker_spec_by_type)


    def _new_worker_name(self, worker_type, worker_number):
        """Returns new worker name.

        This can be overriden in SpecCluster derived classes to customise the
        worker names.
        """
        return f"{worker_type}-{worker_number}"

    # modified but may want to change worker names
    def new_worker_spec(self, worker_type):
        """Return name and spec for the next worker based on worker_spec

        Parameters
        ----------
        worker_type: str name of worker type for the new worker

        Returns
        -------
        d: dict mapping names to worker specs

        See Also
        --------
        scale

        """
        i = 0
        new_worker_name = self._new_worker_name(worker_type, i)
        while new_worker_name in self.worker_spec:
            i += 1
            new_worker_name = self._new_worker_name(worker_type, i)
        print("new worker = ", {new_worker_name: self.new_worker_spec_by_type[worker_type]})
        return {new_worker_name: self.new_worker_spec_by_type[worker_type]}


    def _threads_per_worker(self, worker_type) -> int:
        """Return the number of threads per worker for new workers"""
        if not self.new_worker_spec_by_type:  # pragma: no cover
            raise ValueError("To scale by cores= you must specify cores per worker")

        for name in ["nthreads", "ncores", "threads", "cores"]:
            with suppress(KeyError):
                return self.new_worker_spec_by_type[worker_type]["options"][name]
        assert False, "unreachable"

    def _memory_per_worker(self, worker_type) -> int:
        """Return the memory limit per worker for new workers"""
        if not self.new_worker_spec_by_type:  # pragma: no cover
            raise ValueError(
                "to scale by memory= your worker definition must include a "
                "memory_limit definition"
            )

        for name in ["memory_limit", "memory"]:
            with suppress(KeyError):
                return parse_bytes(self.new_worker_spec_by_type[worker_type]["options"][name])

        raise ValueError(
            "to use scale(memory=...) your worker definition must include a "
            "memory_limit definition"
        )

    # scaled by worker_type
    def scale(self, worker_type, n=0, memory=None, cores=None):
        ''' PARAMETERS
            ----------
            n = scale by num workers
            memory = scale by memory
            cores = scale by cores
        '''
        if memory is not None:
            n = max(n, int(math.ceil(parse_bytes(memory) / self._memory_per_worker(worker_type))))

        if cores is not None:
            n = max(n, int(math.ceil(cores / self._threads_per_worker(worker_type))))

        # Get the number of workers of that worker_type currently
        num_workers_of_type = 0
        for key in self.worker_spec.keys():
            if worker_type in key:
                num_workers_of_type += 1
        print("num_workers_of_type =", num_workers_of_type)


        # Scale back any workers that have not launched yet
        if num_workers_of_type > n:
            not_yet_launched = set(self.worker_spec) - {
                v["name"] for v in self.scheduler_info["workers"].values()
            }
            while num_workers_of_type > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]
                num_workers_of_type -= 1

        
        # as of Python 3.7, dictionary iterates in insertion 
        # order
        # pops the item most recently inserted to dictionary
        # of that type of worker 
        # assumes the type of worker is listed in the worker
        # name
        for key in reversed(list(self.worker_spec.keys())):
            if num_workers_of_type > n:
                if worker_type in key:
                    self.worker_spec.pop(key)
                    num_workers_of_type -= 1
            else:
                break

        print("status =", self.status)
        if self.status not in (Status.closing, Status.closed):
            while num_workers_of_type < n:
                self.worker_spec.update(self.new_worker_spec(worker_type))
                num_workers_of_type += 1

        # makes the workers consistent with that in the spec
        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()


    @property
    def _supports_scaling(self):
        # MODIFIED from SpecCluster
        # scaling is supported if there is a populated 
        # new_worker_spec_by_type dictionary
        return bool(self.new_worker_spec_by_type)

    # not yet modified
    async def scale_down(self, worker_type, workers):
        '''
        # We may have groups, if so, map worker addresses to job names
        if not all(w in self.worker_spec for w in workers):
            mapping = {}
            for name, spec in self.worker_spec.items():
                if "group" in spec:
                    for suffix in spec["group"]:
                        mapping[str(name) + suffix] = name
                else:
                    mapping[name] = name

            workers = {mapping.get(w, w) for w in workers}

        for w in workers:
            if w in self.worker_spec:
                del self.worker_spec[w]
        await self
        '''
        raise Exception("NotImplementedError")


    scale_up = scale 

    def adapt(self,
        *args,
        minimum=0,
        maximum=math.inf,
        minimum_cores: int = None,
        maximum_cores: int = None,
        minimum_memory: str = None,
        maximum_memory: str = None,
        **kwargs,
    ) -> 'Adaptive':
        raise Exception("NotImplementedError")

logger = logging.getLogger(__name__)

job_parameters = """
    cores : int
        Total number of cores per job
    memory: str
        Total amount of memory per job
    processes : int
        Cut the job up into this many processes. Good for GIL workloads or for
        nodes with many cores.
        By default, ``process ~= sqrt(cores)`` so that the number of processes
        and the number of threads per process is roughly the same.
    interface : str
        Network interface like 'eth0' or 'ib0'. This will be used both for the
        Dask scheduler and the Dask workers interface. If you need a different
        interface for the Dask scheduler you can pass it through
        the ``scheduler_options`` argument:
        ``interface=your_worker_interface, scheduler_options={'interface': your_scheduler_interface}``.
    nanny : bool
        Whether or not to start a nanny process
    local_directory : str
        Dask worker local directory for file spilling.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    extra : list
        Deprecated: use ``worker_extra_args`` instead. This parameter will be removed in a future version.
    worker_extra_args : list
        Additional arguments to pass to `dask-worker`
    env_extra : list
        Deprecated: use ``job_script_prologue`` instead. This parameter will be removed in a future version.
    job_script_prologue : list
        Other commands to add to script before launching worker.
    header_skip : list
        Deprecated: use ``job_directives_skip`` instead. This parameter will be removed in a future version.
    job_directives_skip : list
        Directives to skip in the generated job script header.
        Directives lines containing the specified strings will be removed.
        Directives added by job_extra_directives won't be affected.
    log_directory : str
        Directory to use for job scheduler logs.
    shebang : str
        Path to desired interpreter for your batch submission script.
    python : str
        Python executable used to launch Dask workers.
        Defaults to the Python that is submitting these jobs
    config_name : str
        Section to use from jobqueue.yaml configuration file.
    name : str
        Name of Dask worker.  This is typically set by the Cluster
""".strip()


cluster_parameters = """
    n_workers : int
        Number of workers to start by default.  Defaults to 0.
        See the scale method
    silence_logs : str
        Log level like "debug", "info", or "error" to emit here if the
        scheduler is started locally
    asynchronous : bool
        Whether or not to run this cluster object with the async/await syntax
    security : Security or Bool
        A dask.distributed security object if you're using TLS/SSL.  If True,
        temporary self-signed credentials will be created automatically.
    scheduler_options : dict
        Used to pass additional arguments to Dask Scheduler. For example use
        ``scheduler_options={'dashboard_address': ':12435'}`` to specify which
        port the web dashboard should use or ``scheduler_options={'host': 'your-host'}``
        to specify the host the Dask scheduler should run on. See
        :class:`distributed.Scheduler` for more details.
    scheduler_cls : type
        Changes the class of the used Dask Scheduler. Defaults to  Dask's
        :class:`distributed.Scheduler`.
    shared_temp_directory : str
        Shared directory between scheduler and worker (used for example by temporary
        security certificates) defaults to current working directory if not set.
""".strip()


class Job(ProcessInterface, abc.ABC):
    """ Base class to launch Dask workers on Job queues
    This class should not be used directly, use a class appropriate for
    your queueing system (e.g. PBScluster or SLURMCluster) instead.
    Parameters
    ----------
    {job_parameters}
    job_extra : list or dict
        Deprecated: use ``job_extra_directives`` instead. This parameter will be removed in a future version.
    job_extra_directives : list or dict
        Unused in this base class:
        List or dict of other options for the queueing system. See derived classes for specific descriptions.
    Attributes
    ----------
    submit_command: str
        Abstract attribute for job scheduler submit command,
        should be overridden
    cancel_command: str
        Abstract attribute for job scheduler cancel command,
        should be overridden
    See Also
    --------
    PBSCluster
    SLURMCluster
    SGECluster
    OARCluster
    LSFCluster
    MoabCluster
    """.format(
        job_parameters=job_parameters
    )

    _script_template = """
%(shebang)s
%(job_header)s
%(job_script_prologue)s
%(worker_command)s
""".lstrip()

    # Following class attributes should be overridden by extending classes.
    submit_command = None
    cancel_command = None
    config_name = None
    job_id_regexp = r"(?P<job_id>\d+)"

    @abc.abstractmethod
    def __init__(
        self,
        scheduler=None,
        name=None,
        cores=None,
        memory=None,
        processes=None,
        nanny=True,
        protocol=None,
        security=None,
        interface=None,
        death_timeout=None,
        local_directory=None,
        extra=None,
        worker_extra_args=None,
        job_extra=None,
        job_extra_directives=None,
        env_extra=None,
        job_script_prologue=None,
        header_skip=None,
        job_directives_skip=None,
        log_directory=None,
        shebang=None,
        python=sys.executable,
        job_name=None,
        config_name=None,
    ):
        self.scheduler = scheduler
        self.job_id = None

        super().__init__()

        default_config_name = self.default_config_name()
        if config_name is None:
            config_name = default_config_name
        self.config_name = config_name

        if cores is None:
            cores = dask.config.get("jobqueue.%s.cores" % self.config_name)
        if memory is None:
            memory = dask.config.get("jobqueue.%s.memory" % self.config_name)

        if cores is None or memory is None:
            job_class_name = self.__class__.__name__
            cluster_class_name = job_class_name.replace("Job", "Cluster")
            raise ValueError(
                "You must specify how much cores and memory per job you want to use, for example:\n"
                "cluster = {}(cores={}, memory={!r})".format(
                    cluster_class_name, cores or 8, memory or "24GB"
                )
            )

        if job_name is None:
            job_name = dask.config.get("jobqueue.%s.name" % self.config_name)
        if processes is None:
            processes = dask.config.get("jobqueue.%s.processes" % self.config_name)
            if processes is None:
                processes, _ = nprocesses_nthreads(cores)
        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % self.config_name)
        if death_timeout is None:
            death_timeout = dask.config.get(
                "jobqueue.%s.death-timeout" % self.config_name
            )
        if local_directory is None:
            local_directory = dask.config.get(
                "jobqueue.%s.local-directory" % self.config_name
            )
        if extra is None:
            extra = dask.config.get("jobqueue.%s.extra" % self.config_name)
        if worker_extra_args is None:
            worker_extra_args = dask.config.get(
                "jobqueue.%s.worker-extra-args" % self.config_name
            )
        if extra is not None:
            warn = (
                "extra has been renamed to worker_extra_args. "
                "You are still using it (even if only set to []; please also check config files). "
                "If you did not set worker_extra_args yet, extra will be respected for now, "
                "but it will be removed in a future release. "
                "If you already set worker_extra_args, extra is ignored and you can remove it."
            )
            warnings.warn(warn, FutureWarning)
            if not worker_extra_args:
                worker_extra_args = extra

        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name, [])
        if job_extra_directives is None:
            job_extra_directives = dask.config.get(
                "jobqueue.%s.job-extra-directives" % self.config_name, []
            )
        if job_extra is not None:
            warn = (
                "job_extra has been renamed to job_extra_directives. "
                "You are still using it (even if only set to []; please also check config files). "
                "If you did not set job_extra_directives yet, job_extra will be respected for now, "
                "but it will be removed in a future release. "
                "If you already set job_extra_directives, job_extra is ignored and you can remove it."
            )
            warnings.warn(warn, FutureWarning)
            if not job_extra_directives:
                job_extra_directives = job_extra
        self.job_extra_directives = job_extra_directives

        if env_extra is None:
            env_extra = dask.config.get("jobqueue.%s.env-extra" % self.config_name)
        if job_script_prologue is None:
            job_script_prologue = dask.config.get(
                "jobqueue.%s.job-script-prologue" % self.config_name
            )
        if env_extra is not None:
            warn = (
                "env_extra has been renamed to job_script_prologue. "
                "You are still using it (even if only set to []; please also check config files). "
                "If you did not set job_script_prologue yet, env_extra will be respected for now, "
                "but it will be removed in a future release. "
                "If you already set job_script_prologue, env_extra is ignored and you can remove it."
            )
            warnings.warn(warn, FutureWarning)
            if not job_script_prologue:
                job_script_prologue = env_extra
        if header_skip is None:
            header_skip = dask.config.get(
                "jobqueue.%s.header-skip" % self.config_name, None
            )
        if job_directives_skip is None:
            job_directives_skip = dask.config.get(
                "jobqueue.%s.job-directives-skip" % self.config_name, ()
            )
        if header_skip is not None:
            warn = (
                "header_skip has been renamed to job_directives_skip. "
                "You are still using it (even if only set to []; please also check config files). "
                "If you did not set job_directives_skip yet, header_skip will be respected for now, "
                "but it will be removed in a future release. "
                "If you already set job_directives_skip, header_skip is ignored and you can remove it."
            )
            warnings.warn(warn, FutureWarning)
            if not job_directives_skip:
                job_directives_skip = header_skip
        self.job_directives_skip = set(job_directives_skip)

        if log_directory is None:
            log_directory = dask.config.get(
                "jobqueue.%s.log-directory" % self.config_name
            )
        if shebang is None:
            shebang = dask.config.get("jobqueue.%s.shebang" % self.config_name)

        # This attribute should be set in the derived class
        self.job_header = None

        if interface:
            worker_extra_args += ["--interface", interface]
        if protocol:
            worker_extra_args += ["--protocol", protocol]
        if security:
            worker_security_dict = security.get_tls_config_for_role("worker")
            security_command_line_list = [
                ["--tls-" + key.replace("_", "-"), value]
                for key, value in worker_security_dict.items()
                # 'ciphers' parameter does not have a command-line equivalent
                if key != "ciphers"
            ]
            security_command_line = sum(security_command_line_list, [])
            worker_extra_args += security_command_line

        # Keep information on process, cores, and memory, for use in subclasses
        self.worker_memory = parse_bytes(memory) if memory is not None else None
        self.worker_processes = processes
        self.worker_cores = cores
        self.name = name
        self.job_name = job_name

        self.shebang = shebang

        self._job_script_prologue = job_script_prologue

        # dask-worker command line build
        dask_worker_command = "%(python)s -m distributed.cli.dask_worker" % dict(
            python=python
        )
        command_args = [dask_worker_command, self.scheduler]
        command_args += ["--nthreads", self.worker_process_threads]
        if processes is not None and processes > 1:
            command_args += ["--nworkers", processes]

        command_args += ["--memory-limit", self.worker_process_memory]
        command_args += ["--name", str(name)]
        command_args += ["--nanny" if nanny else "--no-nanny"]

        if death_timeout is not None:
            command_args += ["--death-timeout", death_timeout]
        if local_directory is not None:
            command_args += ["--local-directory", local_directory]
        if worker_extra_args is not None:
            command_args += worker_extra_args

        self._command_template = " ".join(map(str, command_args))

        self.log_directory = log_directory
        if self.log_directory is not None:
            if not os.path.exists(self.log_directory):
                os.makedirs(self.log_directory)

    @classmethod
    def default_config_name(cls):
        config_name = getattr(cls, "config_name", None)
        if config_name is None:
            raise ValueError(
                "The class {} is required to have a 'config_name' class variable.\n"
                "If you have created this class, please add a 'config_name' class variable.\n"
                "If not this may be a bug, feel free to create an issue at: "
                "https://github.com/dask/dask-jobqueue/issues/new".format(cls)
            )
        return config_name

    def job_script(self):
        """Construct a job submission script"""
        pieces = {
            "shebang": self.shebang,
            "job_header": self.job_header,
            "job_script_prologue": "\n".join(filter(None, self._job_script_prologue)),
            "worker_command": self._command_template,
        }
        return self._script_template % pieces

    @contextmanager
    def job_file(self):
        """Write job submission script to temporary file"""
        with tmpfile(extension="sh") as fn:
            with open(fn, "w") as f:
                logger.debug("writing job script: \n%s", self.job_script())
                f.write(self.job_script())
            yield fn

    async def _submit_job(self, script_filename):
        # Should we make this async friendly?
        return self._call(shlex.split(self.submit_command) + [script_filename])

    @property
    def worker_process_threads(self):
        return int(self.worker_cores / self.worker_processes)

    @property
    def worker_process_memory(self):
        mem = format_bytes(self.worker_memory / self.worker_processes)
        mem = mem.replace(" ", "")
        return mem

    async def start(self):
        """Start workers and point them to our local scheduler"""
        logger.debug("Starting worker: %s", self.name)

        with self.job_file() as fn:
            out = await self._submit_job(fn)
            self.job_id = self._job_id_from_submit_output(out)

        weakref.finalize(self, self._close_job, self.job_id, self.cancel_command)

        logger.debug("Starting job: %s", self.job_id)
        await super().start()

    def _job_id_from_submit_output(self, out):
        match = re.search(self.job_id_regexp, out)
        if match is None:
            msg = (
                "Could not parse job id from submission command "
                "output.\nJob id regexp is {!r}\nSubmission command "
                "output is:\n{}".format(self.job_id_regexp, out)
            )
            raise ValueError(msg)

        job_id = match.groupdict().get("job_id")
        if job_id is None:
            msg = (
                "You need to use a 'job_id' named group in your regexp, e.g. "
                "r'(?P<job_id>\\d+)'. Your regexp was: "
                "{!r}".format(self.job_id_regexp)
            )
            raise ValueError(msg)

        return job_id

    async def close(self):
        logger.debug("Stopping worker: %s job: %s", self.name, self.job_id)
        self._close_job(self.job_id, self.cancel_command)

    @classmethod
    def _close_job(cls, job_id, cancel_command):
        if job_id:
            with suppress(RuntimeError):  # deleting job when job already gone
                cls._call(shlex.split(cancel_command) + [job_id])
            logger.debug("Closed job %s", job_id)

    @staticmethod
    def _call(cmd, **kwargs):
        """Call a command using subprocess.Popen.
        This centralizes calls out to the command line, providing consistent
        outputs, logging, and an opportunity to go asynchronous in the future.
        Parameters
        ----------
        cmd: List(str))
            A command, each of which is a list of strings to hand to
            subprocess.Popen
        Examples
        --------
        >>> self._call(['ls', '/foo'])
        Returns
        -------
        The stdout produced by the command, as string.
        Raises
        ------
        RuntimeError if the command exits with a non-zero exit code
        """
        cmd_str = " ".join(cmd)
        logger.debug(
            "Executing the following command to command line\n{}".format(cmd_str)
        )

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
        )

        out, err = proc.communicate()
        out, err = out.decode(), err.decode()

        if proc.returncode != 0:
            raise RuntimeError(
                "Command exited with non-zero exit code.\n"
                "Exit code: {}\n"
                "Command:\n{}\n"
                "stdout:\n{}\n"
                "stderr:\n{}\n".format(proc.returncode, cmd_str, out, err)
            )
        return out

class MultiJobQueueCluster(MultiSpecCluster):

    def __init__(
        self,
        n_workers=0,
        job_cls: Job = None,
        # Cluster keywords
        loop=None,
        security=None,
        shared_temp_directory=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        # Scheduler-only keywords
        dashboard_address=None,
        host=None,
        scheduler_options=None,
        scheduler_cls=Scheduler,  # Use local scheduler for now
        # Options for both scheduler and workers
        interface=None,
        protocol=None,
        # Job keywords
        config_name=None,
        new_worker_spec_by_type=None,
        **job_kwargs
    ):
        self.status = Status.created

        default_job_cls = getattr(type(self), "job_cls", None)
        self.job_cls = default_job_cls
        if job_cls is not None:
            self.job_cls = job_cls

        if self.job_cls is None:
            raise ValueError(
                "You need to specify a Job type. Two cases:\n"
                "- you are inheriting from JobQueueCluster (most likely): you need to add a 'job_cls' class variable "
                "in your JobQueueCluster-derived class {}\n"
                "- you are using JobQueueCluster directly (less likely, only useful for tests): "
                "please explicitly pass a Job type through the 'job_cls' parameter.".format(
                    type(self)
                )
            )

        if dashboard_address is not None:
            raise ValueError(
                "Please pass 'dashboard_address' through 'scheduler_options': use\n"
                'cluster = {0}(..., scheduler_options={{"dashboard_address": ":12345"}}) rather than\n'
                'cluster = {0}(..., dashboard_address="12435")'.format(
                    self.__class__.__name__
                )
            )

        if host is not None:
            raise ValueError(
                "Please pass 'host' through 'scheduler_options': use\n"
                'cluster = {0}(..., scheduler_options={{"host": "your-host"}}) rather than\n'
                'cluster = {0}(..., host="your-host")'.format(self.__class__.__name__)
            )

        default_config_name = self.job_cls.default_config_name()
        if config_name is None:
            config_name = default_config_name

        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % config_name)
        if scheduler_options is None:
            scheduler_options = dask.config.get(
                "jobqueue.%s.scheduler-options" % config_name, {}
            )

        if protocol is None and security is not None:
            protocol = "tls://"

        if security is True:
            try:
                security = Security.temporary()
            except ImportError:
                raise ImportError(
                    "In order to use TLS without pregenerated certificates `cryptography` is required,"
                    "please install it using either pip or conda"
                )

        default_scheduler_options = {
            "protocol": protocol,
            "dashboard_address": ":8787",
            "security": security,
        }

        # scheduler_options overrides parameters common to both workers and scheduler
        scheduler_options = dict(default_scheduler_options, **scheduler_options)

        # Use the same network interface as the workers if scheduler ip has not
        # been set through scheduler_options via 'host' or 'interface'
        if "host" not in scheduler_options and "interface" not in scheduler_options:
            scheduler_options["interface"] = interface

        scheduler = {
            "cls": scheduler_cls,
            "options": scheduler_options,
        }

        if shared_temp_directory is None:
            shared_temp_directory = dask.config.get(
                "jobqueue.%s.shared-temp-directory" % config_name
            )
        self.shared_temp_directory = shared_temp_directory

        job_kwargs["config_name"] = config_name
        job_kwargs["interface"] = interface
        job_kwargs["protocol"] = protocol
        job_kwargs["security"] = self._get_worker_security(security)

        # this should be a dictionary of dictionaries where each dictionary
        # represents the job kwargs of the worker spec
        self._job_kwargs = job_kwargs 

        # this should be workerS
        worker = {"cls": self.job_cls, "options": self._job_kwargs}

        # work through multiple job_kwargs
        if "processes" in self._job_kwargs and self._job_kwargs["processes"] > 1:
            worker["group"] = [
                "-" + str(i) for i in range(self._job_kwargs["processes"])
            ]

        self._dummy_job  # trigger property to ensure that the job is valid

        super().__init__(
            scheduler=scheduler,
            worker=worker,
            loop=loop,
            security=security,
            silence_logs=silence_logs,
            asynchronous=asynchronous,
            name=name,
            new_worker_spec_by_type=new_worker_spec_by_type,
        )

        #must change to worker types or remove n_workers
        if n_workers:
            self.scale(n_workers)

    @classmethod
    def default_config_name(cls):
        config_name = getattr(cls, "config_name", None)
        if config_name is None:
            raise ValueError(
                "The class {} is required to have a 'config_name' class variable.\n"
                "If you have created this class, please add a 'config_name' class variable.\n"
                "If not this may be a bug, feel free to create an issue at: "
                "https://github.com/dask/dask-jobqueue/issues/new".format(cls)
            )
        return config_name

    def job_script(self):
        """Construct a job submission script"""
        header = "\n".join(
            [
                line
                for line in self.job_header.split("\n")
                if not any(skip in line for skip in self.header_skip)
            ]
        )
        pieces = {
            "shebang": self.shebang,
            "job_header": header,
            "env_header": self._env_header,
            "worker_command": self._command_template,
        }
        return self._script_template % pieces

    @contextmanager
    def job_file(self):
        """Write job submission script to temporary file"""
        with tmpfile(extension="sh") as fn:
            with open(fn, "w") as f:
                logger.debug("writing job script: \n%s", self.job_script())
                f.write(self.job_script())
            yield fn

    async def _submit_job(self, script_filename):
        # Should we make this async friendly?
        return self._call(shlex.split(self.submit_command) + [script_filename])

    @property
    def worker_process_threads(self):
        return int(self.worker_cores / self.worker_processes)

    @property
    def worker_process_memory(self):
        mem = format_bytes(self.worker_memory / self.worker_processes)
        mem = mem.replace(" ", "")
        return mem

    async def start(self):
        """Start workers and point them to our local scheduler"""
        logger.debug("Starting worker: %s", self.name)

        with self.job_file() as fn:
            out = await self._submit_job(fn)
            self.job_id = self._job_id_from_submit_output(out)

        weakref.finalize(self, self._close_job, self.job_id, self.cancel_command)

        logger.debug("Starting job: %s", self.job_id)
        await super().start()


    # differ for each job type
    @property
    def job_header(self):
        return self._dummy_job.job_header

    # differ for each job type
    def job_script(self):
        return self._dummy_job.job_script()

    # differ for each job type
    @property
    def job_name(self):
        return self._dummy_job.job_name

    def _new_worker_name(self, worker_type, worker_number):
        return "{cluster_name}-{worker_type}-{worker_number}".format(
            cluster_name=self._name,worker_type= worker_type, worker_number=worker_number
        )

    # differ for each job type
    def scale(self, worker_type, n=None, jobs=0, memory=None, cores=None):
        """Scale cluster to specified configurations.
        Parameters
        ----------
        worker_type: str
           Worker specification type to scale
        n : int
           Target number of workers
        jobs : int
           Target number of jobs
        memory : str
           Target amount of memory
        cores : int
           Target number of cores
        """
        if n is not None:f
            jobs = int(math.ceil(n / self._dummy_job.worker_processes))
        return super().scale(worker_type, n=jobs, memory=memory, cores=cores)

