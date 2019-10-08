import socket
import threading
import os


class ClientsServer(threading.Thread):
    socket = None
    # Currently connected users: {user_id -> writer, ...}
    clients = {}
    is_listening = False

    def __init__(self, host='', port=9099):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port

    def run(self):
        """Start a server listening for client connections on the given host:port.
        For every new client connection a short-living thread is created 
        to receive a user id and registed the client connection under that id. 
        """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.bind((self.host, self.port))
        except Exception as err:
            print(f'Failed to start clients server on {self.host}:{self.port}: {err!r}')
            os._exit(1)

        self.socket.listen()
        print(f"Clients: accepting connections on {self.host}:{self.port}")

        # Accept client connections
        self.is_listening = True
        while self.is_listening:
            try:
                conn, _ = self.socket.accept()
            except:
                break
            t = threading.Thread(target=self.register_client, args=(conn,), daemon=True)
            t.start()

    def stop(self):
        """Stop the server by closing all active connections."""
        self.is_listening = False
        if self.socket:
            self.socket.close()
            self.socket = None
        
        i = 0
        for conn in self.clients.values():
            i += 1
            conn.close()
        self.clients.clear()
        print(f'Clients: stopped, closed {i} connections')

    def register_client(self, conn):
        """Receive a user id from a connection and register it as a connected user."""
        data = conn.recv(4096)
        user_id = int(data.decode().rstrip())
        self.clients[user_id] = conn

    def notify(self, ids, message):
        """Forward an event to users.
        Event is forwarded only to currently connected users.

        ids - a list of user ids.
        message - an exact event message.
        """

        if not self.is_listening:
            return

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
