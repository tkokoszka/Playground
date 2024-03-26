import asyncio
import logging
import sys
from typing import Coroutine, Callable


def print_tasks():
    # This function must be executed from within a context that has a running event loop.
    msg = []
    for t in sorted(asyncio.all_tasks(), key=lambda x: x.get_name()):
        is_current = "*" if t == asyncio.current_task() else ""
        msg.append(f"  task={t.get_name()} {is_current}")
    logging.info("\n" + "\n".join(msg))


async def do_something(i: int):
    logging.info(f"({i}) start")
    await asyncio.sleep(1)
    logging.info(f"({i}) end")


async def with_coroutines():
    logging.info("Start")
    c = []
    for i in range(3):
        c.append(do_something(i))

    # No tasks at this stage, they will be scheduled in gather.
    print_tasks()
    logging.info("Sleep")
    await asyncio.sleep(2)
    logging.info("Gather")
    await asyncio.gather(*c)
    logging.info("End")


async def with_tasks():
    logging.info("Start")
    c = []
    for i in range(3):
        c.append(asyncio.create_task(do_something(i), name=f"do_something({i})"))

    # Tasks are already ready created and ready to run.
    print_tasks()
    logging.info("Sleep")
    await asyncio.sleep(2)
    logging.info("Gather")
    await asyncio.gather(*c)
    logging.info("End")


async def with_tasks_noawait():
    logging.info("Start")
    c = []
    for i in range(3):
        c.append(asyncio.create_task(do_something(i), name=f"do_something({i})"))

    # Tasks are already created and ready to run.
    # Since this function has lower sleep that scheduled corounties, coroutines will not complete before program ends.
    print_tasks()
    logging.info("Sleep")
    await asyncio.sleep(0.5)
    logging.info("End")


async def multiple_calls_to_same_coroutine():
    async def coroutine1(msg: str):
        print(f"--> {msg}")
        await asyncio.sleep(1)

    logging.info("Start")

    # Await the same coroutine multiple times.
    c1 = coroutine1("test1")
    await c1
    # await c1  # RuntimeError: cannot reuse already awaited coroutine

    # Unawaited coroutine.
    # Will not fail, but will print RuntimeWarning: coroutine 'multiple_calls_to_same_coroutine.<locals>.coroutine1' was never awaited
    # c2 = coroutine1("test2")

    # Type of the async def - it is a callable returning a coroutine.
    c3: Callable[[str], Coroutine[None, None, None]] = coroutine1
    await c3("test3.1")
    await c3("test3.2")

    logging.info("End")


async def produce_events():
    # Produce X values, then notify that production is completed.
    pass


async def consume_events():
    # Consume X events, expand to consume in batches
    pass


async def producer_consumer():
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(funcName)s|%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stdout)
    # asyncio.run(with_coroutines())
    # asyncio.run(with_tasks())
    # asyncio.run(with_tasks_noawait())
    asyncio.run(multiple_calls_to_same_coroutine())
