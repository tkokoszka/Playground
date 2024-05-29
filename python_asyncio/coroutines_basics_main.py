"""
Basics of asyncio coroutines.
* coroutine type annotation
* ...
"""

import asyncio
import logging
import sys
from collections.abc import Coroutine, Callable, Awaitable, Generator
from typing import Any


async def coroutine_type_annotation():
    """Explanation of coroutine type annotation."""

    async def coroutine1(name: str) -> int:
        """Dummy function, I simply wanted something that has different args and return type."""
        return hash(name)

    # Async function (like the one defined above) returns a Coroutine, its type annotation takes 3 arguments:
    #   Coroutine[YieldType, SendType, ReturnType].
    # Coroutine type annotation is identical to Generator - a generator is a specific type of iterable defined using
    # yield. However, Coroutine and Generator are distinct types.
    logging.info("Return type of an 'async def' is a Coroutine.")
    c1: Coroutine[Any, Any, int] = coroutine1("c1")
    assert isinstance(c1, Coroutine), "c1 is a Coroutine"
    assert not isinstance(c1, Generator), "c1 is not a Generator"

    # Coroutine instances are also instance of Awaitable. In most cases you do not care about details of Generators and
    # you can simplify by using Awaitable as type annotation.
    logging.info("Return type of an 'async def' is a Coroutine and Awaitable.")
    c2: Awaitable[int] = coroutine1("c2")
    assert isinstance(c2, Coroutine), "c2 is a Coroutine"
    assert isinstance(c2, Awaitable), "c2 is an Awaitable"

    # In all the cases above we do not specify arg types because Coroutine does not take arguments. The async
    # function that returns the coroutine takes arguments, this function is a callable.
    logging.info("The 'async def' is a Callable.")
    f_c: Callable[[str], Awaitable[int]] = coroutine1
    assert isinstance(f_c, Callable), "f_c is a Callable"

    # Let's run coroutine to eliminate "coroutine X was never awaited".
    logging.info("Await for created coroutines.")
    asyncio.gather(c1, c2)


def log_tasks():
    """Prints names of all tasks."""
    msg = []
    for t in sorted(asyncio.all_tasks(), key=lambda x: x.get_name()):
        indicator = "*" if t == asyncio.current_task() else " "
        msg.append(f"{indicator}  task={t.get_name()}")
    logging.info("List of tasks:\n" + "\n".join(msg))


async def do_something(i: int):
    logging.info(f"({i}) start")
    await asyncio.sleep(1)
    logging.info(f"({i}) end")


async def with_coroutines():
    """Create and gather coroutines."""
    logging.info("Start")
    c = []
    for i in range(3):
        c.append(do_something(i))

    # No tasks at this stage, they will be scheduled in gather.
    log_tasks()
    logging.info("Sleep")
    await asyncio.sleep(2)
    logging.info("Gather")
    await asyncio.gather(*c)
    logging.info("End")


async def with_tasks():
    """Create and gather tasks."""
    logging.info("Start")
    c = []
    for i in range(3):
        c.append(asyncio.create_task(do_something(i), name=f"do_something({i})"))

    # Tasks are already ready created and ready to run.
    log_tasks()
    logging.info("Sleep")
    await asyncio.sleep(2)
    logging.info("Gather")
    await asyncio.gather(*c)
    logging.info("End")


async def with_tasks_noawait():
    """Create tasks and do not await for them to complete."""
    logging.info("Start")
    c = []
    for i in range(3):
        c.append(asyncio.create_task(do_something(i), name=f"do_something({i})"))

    # Tasks are already created and ready to run.
    # Since this function has lower sleep that scheduled corounties, coroutines will not complete before program ends.
    log_tasks()
    logging.info("Sleep")
    await asyncio.sleep(0.5)
    logging.info("End")


async def multiple_calls_to_same_coroutine():
    """Play with reusing coroutines."""

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


if __name__ == '__main__':
    # Configure logger to print where the logging happened in the code.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(filename)s:%(lineno)d|%(funcName)s()|'
                               '%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stdout)
    asyncio.run(coroutine_type_annotation())
    # asyncio.run(with_coroutines())
    # asyncio.run(with_tasks())
    # asyncio.run(with_tasks_noawait())
    # asyncio.run(multiple_calls_to_same_coroutine())
