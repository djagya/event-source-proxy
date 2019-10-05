import socket
import sys
import threading
import queue
from src.event_handler import EventHandler
from src.source_server import SourceServer
from src.clients_server import ClientsServer

def queue_worker():
    while True:
        message = q.get()
        notify_ids = handler.process(message)
        clients_server.notify(notify_ids, message)
        q.task_done()

q = queue.Queue(maxsize=10000)
handler = EventHandler()
source_server = SourceServer(q)
clients_server = ClientsServer()

try:
    clients_thread = threading.Thread(target=clients_server.listen)
    clients_thread.start()

    worker_thread = threading.Thread(target=queue_worker)
    worker_thread.start()

    source_server.listen()
except (KeyboardInterrupt, SystemExit):
    print("Graceful shutdown")
    # todo: empty the queue
    source_server.stop()
    clients_server.stop()
    sys.exit()
