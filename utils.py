import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from functools import wraps, partial
from typing import Awaitable, Callable, Any

from loguru import logger

executor = ThreadPoolExecutor(32)


def async_in_pool(func: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
    @wraps(func)
    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        logger.debug(f"run {func} in pool")
        loop = asyncio.get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        result = await loop.run_in_executor(executor, pfunc)
        return result

    return _wrapper
