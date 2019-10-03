import asyncio
import concurrent.futures
from src import source_server, clients_server, vars

async def main():
    s_server = await source_server.listen()
    c_server = await clients_server.listen()

    await s_server.serve_forever()
    await c_server.serve_forever()


#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

try:
    asyncio.run(main())
except (KeyboardInterrupt, SystemExit):
    print(
        f'\nTotal messages: {vars.total_messages}')
except:
    raise
