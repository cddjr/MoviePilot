import asyncio
import inspect
import time
from functools import wraps
from aiopathlib import AsyncPath
from pathlib import Path
from typing import Any, Callable

from app.schemas import ImmediateException


def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 2, logger: Any = None):
    """
    :param ExceptionToCheck: 需要捕获的异常
    :param tries: 重试次数
    :param delay: 延迟时间
    :param backoff: 延迟倍数
    :param logger: 日志对象
    """

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ImmediateException:
                    raise
                except ExceptionToCheck as e:
                    msg = f"{str(e)}, {mdelay} 秒后重试 ..."
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        async def async_f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return await f(*args, **kwargs)
                except ImmediateException:
                    raise
                except ExceptionToCheck as e:
                    msg = f"{str(e)}, {mdelay} 秒后重试 ..."
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    await asyncio.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return await f(*args, **kwargs)

        # 根据函数类型返回相应的包装器
        if inspect.iscoroutinefunction(f):
            return async_f_retry
        else:
            return f_retry

    return deco_retry


def log_execution_time(logger: Any = None):
    """
    记录函数执行时间的装饰器
    :param logger: 日志记录器对象，用于记录异常信息
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            msg = f"{func.__name__} execution time: {end_time - start_time:.2f} seconds"
            if logger:
                logger.debug(msg)
            else:
                print(msg)
            return result

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            end_time = time.time()
            msg = f"{func.__name__} execution time: {end_time - start_time:.2f} seconds"
            if logger:
                logger.debug(msg)
            else:
                print(msg)
            return result

        # 根据函数类型返回相应的包装器
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper

    return decorator


def forward_to_async(*, target: Callable):

    def decorator(source: Callable):
        if inspect.iscoroutinefunction(source):
            raise ValueError(
                "forward_to_async decorator cannot be applied to an async function."
            )
        if not inspect.iscoroutinefunction(target):
            pass
            #raise ValueError(
            #    "forward_to_async decorator cannot be forwarded to a non-async function."
            #)

        @wraps(source)
        def wrapper(*args, **kwargs):
            new_args = [AsyncPath(arg) if isinstance(arg, Path) else arg for arg in args]
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop:
                return loop.run_until_complete(target(*new_args, **kwargs))
            else:
                return asyncio.run(target(*new_args, **kwargs))

        # 设置文档字符串
        if not source.__doc__ and target.__doc__:
            wrapper.__doc__ = "以同步的方式调用 " + target.__doc__
        return wrapper

    return decorator
