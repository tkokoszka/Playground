"""
Basics of asyncio coroutines.

Each function defined in this file covers one topic, use Structure/Outline to navigate:
* coroutine_type_annotation - how to type annotate coroutines and why
* coroutine_execution - coroutine vs. task, single threaded
* task_management - explicitly creating tasks from coroutines, cancelling, exceptions
"""

import asyncio
import logging
import sys
import time
from collections.abc import Coroutine, Callable, Awaitable, Generator
from enum import Enum
from typing import Any, List


async def coroutine_type_annotation():
    """Explanation of coroutine type annotation."""

    async def coroutine1(name: str) -> int:
        """Dummy function that has different args and return type."""
        return hash(name)

    # Async function (like the one defined above) returns a Coroutine, its type
    # annotation takes 3 arguments: Coroutine[YieldType, SendType, ReturnType].
    # Coroutine type annotation is identical to Generator, but those are distinct types.
    # A generator is a specific kind of iterable defined using yield.
    logging.info("Return type of an 'async def' is a Coroutine.")
    c1: Coroutine[Any, Any, int] = coroutine1("c1")
    assert isinstance(c1, Coroutine), "c1 is a Coroutine"
    assert not isinstance(c1, Generator), "c1 is not a Generator"

    # Coroutine instances are also instance of Awaitable. In most cases you do not care
    # about details of Generators and you can simplify by using Awaitable as type
    # annotation.
    logging.info("Return type of an 'async def' is a Coroutine and Awaitable.")
    c2: Awaitable[int] = coroutine1("c2")
    assert isinstance(c2, Coroutine), "c2 is a Coroutine"
    assert isinstance(c2, Awaitable), "c2 is an Awaitable"

    # In all the cases above we do not specify arg types because Coroutine does not take
    # arguments. The async function that returns the coroutine takes arguments, this
    # function is a callable.
    logging.info("The 'async def' is a Callable.")
    f_c: Callable[[str], Awaitable[int]] = coroutine1
    assert isinstance(f_c, Callable), "f_c is a Callable"

    # Let's run coroutine to eliminate "coroutine X was never awaited".
    await asyncio.gather(c1, c2)


async def coroutine_execution():
    """Explanation of coroutine execution: scheduling, coroutine vs. task."""
    run_counter: int = 0

    async def coroutine1(msg: str) -> int:
        nonlocal run_counter
        run_counter += 1
        return hash(msg)

    logging.info("Creating a coroutine does not execute it yet, you need to await.")
    # Calling 'async def' function creates a coroutine but does not execute it.
    # Await on coroutine wraps coroutine into a task, enables the task to be scheduled,
    # and waits for the task to complete.
    c1: Awaitable[int] = coroutine1("c1")
    # Yield the current task to give the asyncio scheduler a chance to run c1:
    await asyncio.sleep(0)
    assert run_counter == 0, "C1 was not executed"
    c1_v = await c1
    assert run_counter == 1, "C1 was executed"
    assert c1_v == hash("c1"), "C1 result is as expected"

    try:
        _ = await c1
        assert False, "Awaiting C1 again fails"
    except RuntimeError as e:
        logging.info(
            "Awaiting the same coroutine multiple time raises '%s: %s'",
            type(e).__name__,
            e,
        )

    logging.info("Tasks are the actual units of work that executes the coroutine.")
    # Tasks can be created from coroutines and are immediately eligible to be scheduled.
    # One can use this mechanism to create a task, do some work, await.
    c2: Awaitable[int] = coroutine1("c2")
    c2_task = asyncio.create_task(c2)  # c2 is added to the scheduler
    # Yield the current task to give scheduler chance to execute c2:
    await asyncio.sleep(0)
    assert run_counter == 2, "C2 was executed"
    c2_v = await c2_task
    assert c2_v == hash("c2"), "C2 result is as expected"

    c2_task2 = asyncio.create_task(c2)
    try:
        _ = await c2_task2
        assert False, "Awaiting C2 again fails"
    except RuntimeError:
        logging.info(
            "Await the same coroutine multiple times fails, even when wrapped in"
            " separate tasks"
        )

    logging.info(
        "Asyncio module is a single threaded scheduler, "
        "it can schedule another task only when the current task yields"
    )
    # Tasks are ready to be scheduled as soon as created, but since the asyncio is
    # single threaded loop, scheduler picks up pending tasks only after the currently
    # running task yields. Yielding is through await.
    c3_task = asyncio.create_task(coroutine1("c3"))
    time.sleep(0.5)  # note that there is no await, so this is not yielding!
    assert run_counter == 2, "C3 was not executed"
    # Yield the current task to give scheduler chance to execute c2:
    await asyncio.sleep(0)
    assert run_counter == 3, "C3 was executed"
    c3_v = await c3_task
    assert c3_v == hash("c3"), "C3 result is as expected"


async def task_management():
    """Examples how to create and manage tasks."""

    async def coroutine_noop():
        pass

    async def coroutine_raise(raise_msg: str):
        raise RuntimeError(raise_msg)

    async def coroutine_asleep(asleep_secs: float):
        await asyncio.sleep(asleep_secs)

    def task_list_pretty_print(tasks: List[asyncio.Task]) -> str:
        def execution_status(t: asyncio.Task[Any]):
            class Status(Enum):
                """Execution status."""

                EXCEPTION = "EXCEPTION"
                CANCELLED = "CANCELLED"
                DONE = "DONE"
                ACTIVE = "-"

                def __init__(self, short_name):
                    self.short_name = short_name

            status: Status
            if t.cancelled():
                status = Status.CANCELLED
            elif t.done():
                if t.exception() is not None:
                    status = Status.EXCEPTION
                else:
                    status = Status.DONE
            else:
                status = Status.ACTIVE
            width = max(len(m.short_name) for m in Status.__members__.values())
            return status.short_name.center(width)

        result: List[str] = []
        for t in sorted(tasks, key=lambda x: x.get_name()):
            result.append(f"  [{execution_status(t)}], name={t.get_name()}")
        return "\n".join(result)

    # Task is a wrapper around a coroutine that schedules its execution and allows it to
    # run concurrently with other tasks, managing its lifecycle and state.
    # Each task has a name, which by default is 'Task-{n}'.

    logging.info("Creating bunch of tasks and forcing them to different states.")
    tasks: List[asyncio.Task] = []

    asyncio.current_task().set_name("Main")

    for i in range(2):
        tasks.append(asyncio.create_task(coroutine_noop(), name=f"NoOp-{i}"))

    t_cancel = asyncio.create_task(coroutine_noop(), name="NoOpToCancel")
    t_cancel.cancel("Cancelling ad-hoc")
    tasks.append(t_cancel)

    t_exception = asyncio.create_task(
        coroutine_raise("Ad-hoc exception"), name="NoOpToRaise"
    )
    tasks.append(t_exception)

    t_longsleep = asyncio.create_task(coroutine_asleep(60), name="LongAsleep")
    tasks.append(t_longsleep)

    logging.info("Created %d tasks:\n%s", len(tasks), task_list_pretty_print(tasks))
    # Until tasks are scheduled, they do not execute.
    assert not t_cancel.done() and not t_cancel.cancelled()
    assert not t_exception.done()
    assert not t_longsleep.done()

    await asyncio.sleep(0)  # Yield to give scheduler a chance to execute other tasks.

    # Tasks had a chance to be executed, the no-op tasks that do not have await inside
    # them are done now (i.e. tasks that did not yield, see coroutine_noop).
    logging.info(
        "After yielding once, list is as following:\n%s", task_list_pretty_print(tasks)
    )
    assert t_cancel.done() and t_cancel.cancelled()
    assert t_exception.done() and t_exception.exception() is not None

    try:
        await t_cancel
        assert False
    except asyncio.exceptions.CancelledError as e:
        logging.info(
            "Trying to await on cancelled tasks raises '%s'.", type(e).__name__
        )

    logging.info(
        "Program might finish before all tasks are completed,"
        " this is fine and will not raise warnings."
    )
    assert not t_longsleep.done()


if __name__ == "__main__":
    # Configure logger to print where the logging happened in the code.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d|%(levelname)s|%(filename)s:%(lineno)d"
        "|%(funcName)s()|%(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    asyncio.run(coroutine_type_annotation())
    asyncio.run(coroutine_execution())
    asyncio.run(task_management())
