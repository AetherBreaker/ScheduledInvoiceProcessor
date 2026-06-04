from _typeshed import Incomplete
from datetime import datetime
from typing import Any, Self
from apscheduler.executors.base import BaseExecutor
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger as BaseTrigger
from apscheduler.util import (
  check_callable_args as check_callable_args,
  convert_to_datetime as convert_to_datetime,
  get_callable_name as get_callable_name,
  obj_to_ref as obj_to_ref,
  ref_to_obj as ref_to_obj,
)
from collections.abc import Callable, Sequence

UTC: Incomplete

class Job:
  _scheduler: BaseScheduler
  _jobstore_alias: str
  def __init__(self, scheduler: BaseScheduler, id: str | None = None, **kwargs) -> None: ...  # noqa: A002
  def modify(self, **changes) -> Self: ...
  def reschedule(self, trigger: BaseTrigger, **trigger_args) -> Self: ...
  def pause(self) -> Self: ...
  def resume(self) -> Self: ...
  def remove(self) -> None: ...
  @property
  def pending(self) -> bool: ...
  def _get_run_times(self, now: datetime) -> list[datetime]: ...
  def _modify(self, **changes) -> None: ...
  def __getstate__(self) -> dict[str, Any]: ...
  id: str
  func_ref: str
  func: Callable[..., Any]
  trigger: BaseTrigger
  executor: BaseExecutor
  args: Sequence[Any]
  kwargs: dict[str, Any]
  name: str
  misfire_grace_time: int
  coalesce: bool
  max_instances: int
  next_run_time: datetime
  def __setstate__(self, state: dict[str, Any]) -> None: ...
  def __eq__(self, other: object) -> bool: ...
