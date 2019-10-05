import socket
from src.profiler import Profiler
from src.event_handler import parse
import time

MAX_BUFFER_SIZE = 1000
profiler = Profiler()


class SourceServer:
    socket = None
    # Ring buffer with the MAX_BUFFER_SIZE which restricts the amount of memory used.
    # Max size should be determined based on the distance between max arrived event and min unarrived = events needed to be buffered.
    buffer = {}
    # Last processed event seq id.
    last_seq = 0

    def __init__(self, queue):
        self.queue = queue

    def listen(self, host='127.0.0.1', port=9090):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host, port))
        self.socket.listen()
        print(f"Event source: created server, {host}:{port}")
        conn, addr = self.socket.accept()
        print(f"Event source: source connected, {addr}")

        buffer = ''
        while True:
            data = conn.recv(4096).decode()
            if not data:
                self.stop()
                break

            buffer += data
            *messages, buffer = buffer.split('\n')

            self.buffer_messages(messages)

    def stop(self):
        if self.socket:
            self.socket.close()

    def buffer_messages(self, messages):
        for message in messages:
            seq, idx = self.receive(message)

            if seq > 0 and seq % 100000 == 0:
                profiler.display(seq, self.queue)

            # When a subsequent event is received, trigger the sequence processing
            # until next missing event seq is met.
            if seq == self.last_seq + 1:
                for msg in self.collect_sequence(idx):
                    self.queue.put_nowait(msg)

    def receive(self, message):
        seq, *_ = parse(message)
        seq = int(seq)
        idx = buffer_idx(seq)

        # Check if an overwrite occurs in the buffer, meaning it's overflown and events are being lost.
        if idx in self.buffer:
            print(f'Overwrite! {idx} buffer index for seq#{seq} is not empty')

        # If there's a possibility of events being lost or skipped # in the sequence,
        # a timer would be required to flush the buffer and avoid waiting for a missing event.
        self.buffer[idx] = seq, message

        return seq, idx

    def collect_sequence(self, from_idx):
        idx = from_idx
        messages = []
        while idx in self.buffer:
            seq, message = self.buffer.pop(idx)
            idx = buffer_idx(seq + 1)
            self.last_seq = seq
            messages.append(message)

        return messages


# Ring buffer index.
def buffer_idx(seq): return seq % MAX_BUFFER_SIZE
