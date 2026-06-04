from _typeshed import Incomplete
from apscheduler.schedulers.base import BaseScheduler as BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler as BlockingScheduler
from apscheduler.util import asbool as asbool

class BackgroundScheduler(BlockingScheduler):
    _thread: Incomplete
    _daemon: Incomplete
    def _configure(self, config) -> None: ...
    _event: Incomplete
    def start(self, *args, **kwargs) -> None: ...
    def shutdown(self, *args, **kwargs) -> None: ...
