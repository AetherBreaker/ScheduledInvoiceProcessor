from _typeshed import Incomplete
from apscheduler.executors.base import BaseExecutor as BaseExecutor, run_coroutine_job as run_coroutine_job, run_job as run_job
from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import BaseScheduler

class AsyncIOExecutor(BaseExecutor):
  _eventloop: Incomplete
  _pending_futures: Incomplete
  def start(self, scheduler: AsyncIOScheduler | BaseScheduler, alias: str) -> None: ...
  def shutdown(self, wait: bool = True) -> None: ...
  def _do_submit_job(self, job: Job, run_times) -> None: ...
