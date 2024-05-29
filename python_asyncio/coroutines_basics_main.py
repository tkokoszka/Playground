"""
Basics of asyncio coroutines.
* coroutine type annotation
* coroutine execution - coroutine vs. task, single threaded
"""

import asyncio
import logging
import sys
import time
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
    asyncio.gather(c1, c2)


async def coroutine_execution():
    """Explanation of coroutine execution: scheduling, coroutine vs. task."""
    run_counter: int = 0

    async def coroutine1(msg: str) -> int:
        nonlocal run_counter
        run_counter += 1
        return hash(msg)

    logging.info("Creating a coroutine does not execute it yet, you need to await.")
    # Calling 'async def' function creates a coroutine but does not execute it. Await on coroutine wraps coroutine into
    # a task, enables the task to be scheduled, and waits for the task to complete.
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

    logging.info("Tasks are the actual unit of work that executes the coroutine.")
    # Tasks can be created from coroutines, tasks are immediately eligible to be scheduled. One can use this mechanism
    # to create a task, do some work, await.
    c2: Awaitable[int] = coroutine1("c2")
    c2_task = asyncio.create_task(c2)  # c2 is added to the scheduler
    await asyncio.sleep(0)             # yield the current task to give scheduler chance to execute c2
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
    # Tasks are ready to be scheduled as soon as created, but since the asyncio is single threaded loop, scheduler
    # picks up pending tasks only after the currently running task yields. Yielding is through await.
    c3_task = asyncio.create_task(coroutine1("c3"))  # c3_task is created and ready to be scheduled.
    time.sleep(0.5)  # note that there is no await, so this is not yielding!
    assert run_counter == 2, "C3 was not executed"
    await asyncio.sleep(0)  # yield the current task to give scheduler chance to execute c2
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
    # Configure logger to print where the logging happened in the code.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(filename)s:%(lineno)d|%(funcName)s()|'
                               '%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stdout)

    asyncio.run(coroutine_type_annotation())
    asyncio.run(coroutine_execution())
    #asyncio.run(with_coroutines())
    #asyncio.run(with_tasks_noawait())
