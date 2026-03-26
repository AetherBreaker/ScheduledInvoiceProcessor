if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from collections.abc import Callable
from functools import wraps
from logging import getLogger

# from collections.abc import Awaitable


logger = getLogger(__name__)


# def handle_fatal_exc_async[**TP, TR](func: Callable[TP, Awaitable[TR]]) -> Callable[TP, Awaitable[TR]]:
#   @wraps(func)
#   async def wrapper(*args: TP.args, **kwargs: TP.kwargs) -> TR:
#     try:
#       return await func(*args, **kwargs)
#     except BaseException as e:
#       logger.critical(f"Fatal exception in {func.__name__}: {e}", exc_info=True)
#       exit(1)  # Exit with non-zero code to indicate failure to Coolify

#   return wrapper


def handle_fatal_exc[**TP, TR](func: Callable[TP, TR]) -> Callable[TP, TR]:
  @wraps(func)
  def wrapper(*args: TP.args, **kwargs: TP.kwargs) -> TR:
    try:
      return func(*args, **kwargs)
    except BaseException as e:
      logger.critical(f"Fatal exception in {func.__name__}: {e}", exc_info=True)
      exit(1)  # Exit with non-zero code to indicate failure to Coolify

  return wrapper
