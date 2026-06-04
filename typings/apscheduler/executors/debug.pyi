from apscheduler.executors.base import BaseExecutor as BaseExecutor, run_job as run_job

class DebugExecutor(BaseExecutor):
    def _do_submit_job(self, job, run_times) -> None: ...
