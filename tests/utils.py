import asyncio
from functools import wraps


def async_test(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        future = f(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)

    return wrapper
