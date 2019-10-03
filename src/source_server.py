import asyncio
import src.vars
from src.clients_server import handle_event

# todo: measure amount of items in queue and without using queue
async def worker(queue):
    while True:
        event = await queue.get()
        await handle_event(event)
        queue.task_done()

# todo: this should be in one thread, accepting events to a queue, another thred is to send messages from queue to clients
# todo: find out how to run two event loops in threads: https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools
async def connection_handler(reader, writer):
    print(f"Event source: connected")

    #queue = asyncio.Queue()
    #task = asyncio.create_task(worker(queue))

    while True:
        message = await reader.readline()
        if not message:
            break
        message = message.decode().rstrip()
        await handle_event(message)

        #if src.vars.total_messages % 1000 == 0:
        #    print(f'Items in queue: {queue.qsize()}')

        src.vars.total_messages += 1

    #await task


async def listen():
    server = await asyncio.start_server(connection_handler, '127.0.0.1', 9090)
    addr = server.sockets[0].getsockname()
    print(f"Event source: created server, {addr}")

    return server
