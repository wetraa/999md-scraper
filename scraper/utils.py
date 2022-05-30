import asyncio
import json


def amap(func, seq):
    return asyncio.gather(*map(func, seq))


def arun(coro):
    event_loop = asyncio.get_event_loop()
    return event_loop.run_until_complete(coro)


def write_json(data, filename='results.json'):
    with open(filename, 'w') as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))
