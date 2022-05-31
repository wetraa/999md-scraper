import asyncio
from collections import defaultdict
import time
from urllib.parse import urlparse

import aiofiles
from aiohttp.client_exceptions import ClientError
from funcy import decorator


@decorator
async def last_fetch(call):
    resp = await call()
    async with aiofiles.open('last_fetch.html', 'w') as f:
        await f.write(resp.body)
    return resp


@decorator
async def log_fetch(call):
    print('FETCH', call._args[0])
    start_point = time.time()
    resp = await call()
    end_point = time.time()
    print(f'{round(end_point - start_point, 2)} | {resp.status} | {resp.url}')
    return resp


@decorator
async def limit(call, *, concurrency=None, per_domain=None):
    domain = urlparse(call.url).netloc

    if not hasattr(call._func, 'running'):
        call._func.running = defaultdict(set)
    running = call._func.running['']
    running_in_domain = call._func.running[domain]

    while concurrency and len(running) >= concurrency \
            or per_domain and len(running_in_domain) >= per_domain:
        await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        _clean_tasks(running)
        _clean_tasks(running_in_domain)

    this = asyncio.ensure_future(call())
    running.add(this)
    running_in_domain.add(this)
    return await this


def _clean_tasks(running):
    for task in list(running):
        if task.done():
            running.remove(task)


class ValidateError(Exception):
    pass


RETRY_ERRORS = (ClientError, asyncio.TimeoutError, ValidateError)


@decorator
async def retry(call, *, tries=3, errors=RETRY_ERRORS, timeout=3, on_error=print):
    errors = _ensure_exceptable(errors)
    for attempt in range(tries):
        try:
            return await call()
        except errors as e:
            if on_error:
                message = f'{e.__class__.__name__}: {e}' if str(e) else e.__class__.__name__
                on_error(f'\033[31mFailed with {message}, retrying {attempt + 1}/{tries}...\033[0m')
            # Reraise error on last attempt
            if attempt + 1 == tries:
                raise
            else:
                timeout_value = timeout(attempt) if callable(timeout) else timeout
                if timeout_value > 0:
                    await asyncio.sleep(timeout_value)


def _ensure_exceptable(errors):
    is_exception = isinstance(errors, type) and issubclass(errors, BaseException)
    return (errors,) if is_exception else tuple(errors)
