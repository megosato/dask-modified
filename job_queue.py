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

