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

# This variable depends on the maximum arriving number of out-of-order events,
# i.e. the difference between max arrived and min unarrived event seq.
if len(sys.argv) >= 2 and sys.argv[1].startswith('--buffer='):
    buffer_size = int(sys.argv[1].split('=')[1] or 10000)
else:
    buffer_size = 10000

q = queue.Queue(maxsize=buffer_size)
handler = EventHandler()
source_server = SourceServer(q, buffer_size)
clients_server = ClientsServer()

try:
    clients_thread = threading.Thread(target=clients_server.listen, daemon=True)
    clients_thread.start()

    worker_thread = threading.Thread(target=queue_worker, daemon=True)
    worker_thread.start()

    source_server.listen()
    sys.exit()
except Exception as err:
    print(f'Graceful shutdown, reason: {err!r}')
    source_server.stop()
    clients_server.stop()
    sys.exit()