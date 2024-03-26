"""Implement producer/consumer with a buffer."""

import asyncio
import logging


async def produce_events():
    # Produce X values, then notify that production is completed.
    pass


async def consume_events():
    # Consume X events, expand to consume in batches
    pass


async def producer_consumer():
    pass


async def run_machinery():
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d|%(levelname)s|%(funcName)s|%(message)s',
                        datefmt='%H:%M:%S',
                        stream=sys.stdout)
    asyncio.run(run_machinery())
