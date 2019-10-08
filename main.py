import socket
import sys
import threading
import queue
from src.event_handler import EventHandler
from src.source_server import SourceServer
from src.clients_server import ClientsServer

if sys.version_info < (3, 6):
    raise RuntimeError("This package requres Python 3.6+")

def queue_worker():
    while True:
        message = q.get(timeout=5)
        notify_ids = handler.process(message)
        clients_server.notify(notify_ids, message)
        q.task_done()

# This variable depends on the maximum arriving number of out-of-order events,
# i.e. the difference between max arrived and min unarrived event seq.
if len(sys.argv) >= 2 and sys.argv[1].startswith('--buffer='):
    buffer_size = int(sys.argv[1].split('=')[1] or 100000)
else:
    buffer_size = 100000

q = queue.Queue(maxsize=buffer_size)
handler = EventHandler()
source_server = SourceServer(q, buffer_size)
clients_server = ClientsServer()

try:
    clients_server.start()
    source_server.start()

    queue_worker()
    sys.exit()
except BaseException as err:
    print(f'Graceful shutdown, reason: {err!r}')
    source_server.stop()
    clients_server.stop()
    sys.exit()
