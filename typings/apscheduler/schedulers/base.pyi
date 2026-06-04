# Standard library imports
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any, TextIO

# Third party imports
from _typeshed import Incomplete, SupportsRead, SupportsWrite
from apscheduler.events import (
  EVENT_ALL as EVENT_ALL,
  EVENT_ALL_JOBS_REMOVED as EVENT_ALL_JOBS_REMOVED,
  EVENT_EXECUTOR_ADDED as EVENT_EXECUTOR_ADDED,
  EVENT_EXECUTOR_REMOVED as EVENT_EXECUTOR_REMOVED,
  EVENT_JOB_ADDED as EVENT_JOB_ADDED,
  EVENT_JOB_MAX_INSTANCES as EVENT_JOB_MAX_INSTANCES,
  EVENT_JOB_MODIFIED as EVENT_JOB_MODIFIED,
  EVENT_JOB_REMOVED as EVENT_JOB_REMOVED,
  EVENT_JOB_SUBMITTED as EVENT_JOB_SUBMITTED,
  EVENT_JOBSTORE_ADDED as EVENT_JOBSTORE_ADDED,
  EVENT_JOBSTORE_REMOVED as EVENT_JOBSTORE_REMOVED,
  EVENT_SCHEDULER_PAUSED as EVENT_SCHEDULER_PAUSED,
  EVENT_SCHEDULER_RESUMED as EVENT_SCHEDULER_RESUMED,
  EVENT_SCHEDULER_SHUTDOWN as EVENT_SCHEDULER_SHUTDOWN,
  EVENT_SCHEDULER_STARTED as EVENT_SCHEDULER_STARTED,
  JobEvent as JobEvent,
  JobSubmissionEvent as JobSubmissionEvent,
  SchedulerEvent as SchedulerEvent,
)
from apscheduler.executors.base import BaseExecutor as BaseExecutor, MaxInstancesReachedError as MaxInstancesReachedError
from apscheduler.executors.pool import ThreadPoolExecutor as ThreadPoolExecutor
from apscheduler.job import Job as Job
from apscheduler.jobstores.base import (
  BaseJobStore as BaseJobStore,
  ConflictingIdError as ConflictingIdError,
  JobLookupError as JobLookupError,
)
from apscheduler.jobstores.memory import MemoryJobStore as MemoryJobStore
from apscheduler.schedulers import (
  SchedulerAlreadyRunningError as SchedulerAlreadyRunningError,
  SchedulerNotRunningError as SchedulerNotRunningError,
)
from apscheduler.triggers.base import BaseTrigger as BaseTrigger
from apscheduler.util import (
  asbool as asbool,
  asint as asint,
  astimezone as astimezone,
  maybe_ref as maybe_ref,
  obj_to_ref as obj_to_ref,
  ref_to_obj as ref_to_obj,
  undefined as undefined,
)

STATE_STOPPED: int
STATE_RUNNING: int
STATE_PAUSED: int

class BaseScheduler(metaclass=ABCMeta):
  _trigger_plugins: Incomplete
  _executor_plugins: Incomplete
  _jobstore_plugins: Incomplete
  _trigger_classes: Incomplete
  _executor_classes: Incomplete
  _jobstore_classes: Incomplete
  _executors: Incomplete
  _executors_lock: Incomplete
  _jobstores: Incomplete
  _jobstores_lock: Incomplete
  _listeners: Incomplete
  _listeners_lock: Incomplete
  _pending_jobs: Incomplete
  state: Incomplete
  def __init__(self, gconfig: dict[str, Any] = {}, **options) -> None: ...
  def __getstate__(self) -> None: ...
  def configure(self, gconfig: dict = {}, prefix: str = "apscheduler.", **options) -> None: ...
  def start(self, paused: bool = False) -> None: ...
  @abstractmethod
  def shutdown(self, wait: bool = True): ...
  def pause(self) -> None: ...
  def resume(self) -> None: ...
  @property
  def running(self): ...
  def add_executor(self, executor: str | BaseExecutor, alias: str = "default", **executor_opts) -> None: ...
  def remove_executor(self, alias: str, shutdown: bool = True) -> None: ...
  def add_jobstore(self, jobstore: str | BaseJobStore, alias: str = "default", **jobstore_opts) -> None: ...
  def remove_jobstore(self, alias: str, shutdown: bool = True) -> None: ...
  def add_listener(self, callback: Callable[[Any]], mask: int = ...) -> None: ...
  def remove_listener(self, callback: Callable[[Any]]) -> None: ...
  def add_job(
    self,
    func: Callable[..., Any],
    trigger: str | BaseTrigger = None,
    args: Sequence[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    id: str | None = None,  # noqa: A002
    name: str | None = None,
    misfire_grace_time: int = ...,
    coalesce: bool = ...,
    max_instances: int = ...,
    next_run_time: datetime = ...,
    jobstore: str = "default",
    executor: str = "default",
    replace_existing: bool = False,
    **trigger_args,
  ): ...
  def scheduled_job(
    self,
    trigger: str | BaseTrigger = None,
    args: Sequence[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    id: str | None = None,  # noqa: A002
    name: str | None = None,
    misfire_grace_time: int = ...,
    coalesce: bool = ...,
    max_instances: int = ...,
    next_run_time: datetime = ...,
    jobstore: str = "default",
    executor: str = "default",
    **trigger_args,
  ): ...
  def modify_job(self, job_id: str, jobstore: str | None = None, **changes): ...
  def reschedule_job(self, job_id: str, jobstore: str | None = None, trigger: str | BaseTrigger | None = None, **trigger_args): ...
  def pause_job(self, job_id: str, jobstore: str | None = None): ...
  def resume_job(self, job_id: str, jobstore: str | None = None): ...
  def get_jobs(self, jobstore: str | None = None, pending: bool | None = None): ...
  def get_job(self, job_id: str, jobstore: str | None = None): ...
  def remove_job(self, job_id: str, jobstore: str | None = None) -> None: ...
  def remove_all_jobs(self, jobstore: str | None = None) -> None: ...
  def print_jobs(self, jobstore: str | None = None, out: TextIO | None = None) -> None: ...
  def export_jobs(self, outfile: SupportsWrite[str], jobstore: str | None = None): ...
  def import_jobs(self, infile: SupportsRead[str], jobstore: str = "default"): ...
  @abstractmethod
  def wakeup(self): ...
  _logger: Incomplete
  timezone: Incomplete
  jobstore_retry_interval: Incomplete
  _job_defaults: Incomplete
  def _configure(self, config: dict[str, Any]) -> None: ...
  def _create_default_executor(self): ...
  def _create_default_jobstore(self): ...
  def _lookup_executor(self, alias: str): ...
  def _lookup_jobstore(self, alias: str): ...
  def _lookup_job(self, job_id: str, jobstore_alias: str): ...
  def _dispatch_event(self, event: SchedulerEvent) -> None: ...
  def _check_uwsgi(self) -> None: ...
  def _real_add_job(self, job: Job, jobstore_alias: str, replace_existing: bool) -> None: ...
  def _create_plugin_instance(self, type_: type, alias: str, constructor_kwargs: dict[str, Any]): ...
  def _create_trigger(self, trigger: str | BaseTrigger, trigger_args: dict[str, Any]): ...
  def _create_lock(self): ...
  def _process_jobs(self): ...
