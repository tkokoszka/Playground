"""
Basics of asyncio coroutines.
* coroutine type annotation
* coroutine execution - coroutine vs. task, single threaded
"""

import asyncio
import logging
import sys
import time
from collections.abc import Coroutine, Callable, Awaitable
from typing import Any


async def coroutine_type_annotation():
    """Explanation of coroutine type annotation."""

    async def coroutine1(name: str) -> str:
        """Dummy coroutine."""
        return name

    # Async function (like the one defined above) returns a Coroutine, which in turn is a Generator.
    # Coroutine type annotation takes 3 arguments: [YieldType, SendType, ReturnType].
    logging.info("Return type of an 'async def' is a Coroutine.")
    c1: Coroutine[Any, Any, str] = coroutine1("c1")
    assert isinstance(c1, Coroutine), "c1 is a Coroutine"

    # All Coroutine instances are also instance of Awaitable. If you do not care about details of Generators, you
    # can simplify by using Awaitable.
    logging.info("Return type of an 'async def' is a Coroutine and Awaitable.")
    c2: Awaitable[str] = coroutine1("c2")
    assert isinstance(c2, Coroutine), "c2 is a Coroutine"
    assert isinstance(c2, Awaitable), "c2 is an Awaitable"

    # Let's run coroutine to eliminate "coroutine X was never awaited".
    asyncio.gather(c1, c2)


async def coroutine_execution():
    """Explanation of coroutine execution: scheduling, coroutine vs. task."""
    run_counter: int = 0

    async def coroutine1(msg: str) -> int:
        nonlocal run_counter
        run_counter += 1
        return hash(msg)

    logging.info("Creating a coroutine does not run it yet, you need to await.")
    # Calling 'async def' function creates a coroutine but does not run it. Await on coroutine wraps coroutine into a
    # task, enables it to be scheduled, and waits in async mode for the coroutine to complete.
    c1: Awaitable[int] = coroutine1("c1")
    await asyncio.sleep(0)  # yield the current task to give the asyncio scheduler chance to run c1
    assert run_counter == 0, "C1 was not executed"
    c1_v = await c1
    assert run_counter == 1, "C1 was executed"
    assert c1_v == hash("c1"), "C1 result is as expected"

    try:
        _ = await c1
        assert False, "Awaiting C1 again fails"
    except RuntimeError as e:
        logging.info(f"Awaiting the same coroutine multiple time raises '{type(e).__name__}: {e}'")

    logging.info("Tasks are the actual units of work that execute the coroutine.")
    # Tasks can be created from coroutines and are eligible to be scheduled. One can use this mechanism to schedule
    # a task, do some work, and await later.
    c2: Awaitable[int] = coroutine1("c2")
    c2_task = asyncio.create_task(c2)  # c2 is added to the scheduler now
    await asyncio.sleep(0)  # yield the current task to give scheduler chance to run c2
    assert run_counter == 2, "C2 was executed"
    c2_v = await c2_task
    assert c2_v == hash("c2"), "C2 result is as expected"

    c2_task2 = asyncio.create_task(c2)
    try:
        _ = await c2_task2
        assert False, "Awaiting C2 again fails"
    except RuntimeError:
        logging.info("Await the same coroutine multiple times fails, even when wrapped in separate tasks")
        pass

    logging.info("Asyncio module is a single threaded scheduler, "
                 "it can schedule another task only when the current task yields")
    # New tasks are ready to be scheduled as soon as created, but since the asyncio is single threaded loop, scheduler
    # will pick up the task only after currently running task yields. Yielding is through await.
    c3_task = asyncio.create_task(coroutine1("c3"))  # c3_task is created and ready to be scheduled.
    time.sleep(0.5)  # simulate some heavy computation, note that this is not yielding!
    assert run_counter == 2, "C3 was not executed"
    await asyncio.sleep(0)  # yield the current task to give scheduler chance to run c2
    assert run_counter == 3, "C3 was executed"
    c3_v = await c3_task
    assert c3_v == hash("c3"), "C3 result is as expected"


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


if __name__ == '__main__':
    # Configure logger to print python function that did the logging.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(funcName)s|%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stdout)

    asyncio.run(coroutine_type_annotation())
    asyncio.run(coroutine_execution())
    #asyncio.run(with_coroutines())
    # asyncio.run(with_tasks())
    #asyncio.run(with_tasks_noawait())
