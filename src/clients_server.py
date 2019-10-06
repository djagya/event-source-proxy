import socket
import threading


class ClientsServer:
    # Currently connected users: {user_id -> writer, ...}
    clients = {}

    def listen(self, host='127.0.0.1', port=9099):
        """Start a server listening for client connections on the given host:port.
        For every new client connection a short-living thread is created 
        to receive a user id and registed the client connection under that id. 
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen()
        print(f"Clients: accepting connections on {host}:{port}")

        # Accept client connections
        while True:
            conn, _ = s.accept()
            t = threading.Thread(target=self.register_client, args=(conn,))
            t.start()

    def stop(self):
        """Stop the server by closing all active connections."""
        i = 0
        for conn in self.clients.values():
            i += 1
            conn.close()
        self.clients.clear()
        print(f'Clients: stopped, closed {i} connections')

    def register_client(self, conn):
        """Receive a user id from a connection and register it as a connected user."""
        data = conn.recv(4096)
        user_id = data.decode().rstrip()
        self.clients[user_id] = conn

    def notify(self, ids, message):
        """Forward an event to users.
        Event is forwarded only to currently connected users.

        ids - a list of user ids.
        message - an exact event message.
        """

        # All ids for True or only presented in connected clients.
        ids = self.clients.keys() if ids is True else set(ids).intersection(self.clients)
        if not ids:
            return

        for id in ids:
            conn = self.clients[id]
            try:
                conn.sendall((message + '\n').encode())
            except Exception as e:
                print(
                    f'Error on sending data: {e!r}, consider the client disconnected')
                del self.clients[id]
