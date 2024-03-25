import asyncio
import logging
import sys


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
    # Since this function has lower sleep that scheduled corounties, coroutines will not complete.
    print_tasks()
    logging.info("Sleep")
    await asyncio.sleep(0.5)
    logging.info("End")


async def use_condition():
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(funcName)s|%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stderr)
    # asyncio.run(with_coroutines())
    # asyncio.run(with_tasks())
    asyncio.run(with_tasks_noawait())
