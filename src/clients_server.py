import socket
import threading


class ClientsServer:
    # Currently connected users: {user_id -> writer, ...}
    clients = {}

    def listen(self, host='127.0.0.1', port=9099):
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
        for conn in self.clients.values():
            conn.close()

    def register_client(self, conn):
        """Receive a user id from a connected client and register it as a connected client."""
        data = conn.recv(4096)
        user_id = data.decode().rstrip()
        self.clients[user_id] = conn

    def notify(self, ids, message):
        """Send event to the user.
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
                    f'Error on sending data: {e!r}, consider it disconnected')
                del self.clients[id]
