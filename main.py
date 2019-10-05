import asyncio
import concurrent.futures
from src.message_handler import MessageHandler

handler = MessageHandler()

# todo: this should be in one thread, accepting events to a queue, another thred is to send messages from queue to clients
# todo: find out how to run two event loops in threads: https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools

async def listen_source():
    async def connected_cb(reader, writer):
        print(f"Event source: connected")
        while True:
            message = await reader.readline()
            if not message:
                break
            await handler.receive(message.decode().rstrip())

    server = await asyncio.start_server(connected_cb, '127.0.0.1', 9090)
    addr = server.sockets[0].getsockname()
    print(f"Event source: created server, {addr}")

    return server


async def listen_clients():
    async def connected_cb(reader, writer):
        #print(f"Clients: connected user_id={user_id}")
        # Once connected, wait for the id received as a first client message
        user_id = (await reader.readline()).decode().rstrip()
        handler.add_client(user_id, writer)

    server = await asyncio.start_server(connected_cb, '127.0.0.1', 9099)
    addr = server.sockets[0].getsockname()
    print(f"Clients: created server, {addr}")

    return server


async def main():
    s_server = await listen_source()
    c_server = await listen_clients()
    await s_server.serve_forever()
    await c_server.serve_forever()
   

#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
try:
    asyncio.run(main())
except (KeyboardInterrupt, SystemExit):
    print("Finishing current tasks")
    # todo: graceful finish by waiting all tasks.
    #await asyncio.wait(*asyncio.all_tasks())
