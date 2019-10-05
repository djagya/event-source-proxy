import asyncio


class ClientsServer:
    # Currently connected users: {user_id -> writer, ...}
    clients = {}

    def __init__(self, host='127.0.0.1', port=9099):
        self.host = host
        self.port = port

    async def listen(self):
        server = await asyncio.start_server(self.connected_cb, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f"Clients: created server, {addr}")

        await server.serve_forever()

    async def connected_cb(self, reader, writer):
        #print(f"Clients: connected user_id={user_id}")
        # Once connected, wait for the id received as a first client message
        user_id = (await reader.readline()).decode().rstrip()
        self.clients[user_id] = writer

    async def notify(self, event, ids):
        """Send event to the user.
        Client is disconnected if on read the connection fails.
        """
        # All ids for True or only presented in connected clients.
        ids = self.clients.keys() if ids is True else set(ids).intersection(self.clients)
        if not ids:
            return

        #tasks = []
        for id in ids:
            writer = self.clients[id]
            try:
                await writer.drain()
                writer.write(event.encode())
            except (ConnectionError):
                print(f'User#{id} has disconnected')
                del self.clients[id]

            #tasks.append(asyncio.create_task(writer.drain()))

        #await asyncio.gather(*tasks)

    async def send_batch(self, id, messages):
        writer = self.clients[id]
        for message in messages:
            writer.write(message.encode())
        #print(f'Awaiting drain to {id}: {messages!r}')
        #await writer.drain()
        #print(f'Sent to {id}: {messages!r}')