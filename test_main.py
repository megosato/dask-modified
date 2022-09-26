from job_queue import MultiSpecCluster
from dask.distributed import Scheduler, Worker, Nanny


scheduler = {'cls': Scheduler, 'options': {"dashboard_address": ':8787'}}
workers = {}

new_worker_spec = {
	'worker': {"cls": Worker, "options": {"nthreads": 1}},
    'nanny': {"cls": Nanny, "options": {"nthreads": 2}},
}

ms_cluster = MultiSpecCluster(scheduler=scheduler, workers=workers, new_worker_spec_by_type=new_worker_spec)

print(f"new worker specs: {ms_cluster.new_worker_spec_by_type}")
print(f"ms_cluster workers: {ms_cluster.workers}")


ms_cluster.scale('nanny', 2)
print(f"ms_cluster workers: {ms_cluster.workers}")

ms_cluster.scale('worker', 3)
print(f"ms_cluster workers: {ms_cluster.workers}")

ms_cluster.scale('nanny', 1)
print(f"ms_cluster workers: {ms_cluster.workers}")

