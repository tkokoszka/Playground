"""Producer and consumer with a batch mode.

Producer produces events, consumer consumes in batches: waits for X elements but no longer than Y seconds.
"""

import asyncio
import logging
import sys


async def produce_events():
    pass


async def consume_events_with_batch():
    pass


async def run_machinery():
    pass


if __name__ == '__main__':
    # Configure logger to print python function that did the logging.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(funcName)s|%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stdout)
    asyncio.run(run_machinery())
