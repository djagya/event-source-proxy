import socket
from src.profiler import Profiler
from src.event_handler import parse
import time

MAX_BUFFER_SIZE = 1000
profiler = Profiler()


class SourceServer:
    socket = None
    # Ring buffer of the size "buffers_size" which restricts the max amount of used memory.
    # Max size should be determined based on the distance between max arrived event and min unarrived = events needed to be buffered.
    buffer = {}
    # Last processed event seq id.
    last_seq = 0

    def __init__(self, queue, buffer_size):
        self.queue = queue
        self.buffer_size = buffer_size

    def buffer_idx(self, seq):
        """Ring buffer index getter"""
        return seq % self.buffer_size

    def listen(self, host='127.0.0.1', port=9090):
        """Start a server listening for the event source connection on the given host:port.
        Data chunks arrives and consist of out-of-order event messages. 
        Data is then split by \\n to extract a list of messages 
        which are then buffered to ensure the correct processing order.
        """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host, port))
        self.socket.listen()
        print(
            f"Event source: created server on {host}:{port} with the buffer size {self.buffer_size}")
        conn, addr = self.socket.accept()
        print(f"Event source: source connected, {addr}")

        buffer = ''
        while True:
            data = conn.recv(4096).decode()
            if not data:
                self.stop()
                break

            buffer += data
            # Remaing piece of buffered data goes back to buffer.
            *messages, buffer = buffer.split('\n')

            self.buffer_messages(messages)

    def stop(self):
        """Stop the server by closing the active connection."""
        if self.socket:
            self.socket.close()
            self.socket = None
            print('Event source: stopped')

    def buffer_messages(self, messages):
        """Buffer arrived out-of-order messages in a ring-buffer
        until an event arrives with the next sequential id after the last one.
        """
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
        """Receive the message by parsing and putting it on a corresponding ring-buffer index.
        Overflows are prevented in current implementation, considering the event loss is not allowed.Ò
        """
        seq, *_ = parse(message)
        seq = int(seq)
        idx = self.buffer_idx(seq)

        # Check if an overwrite occurs in the buffer, meaning it's overflown and events are being lost.
        if idx in self.buffer:
            raise BufferError(
                f'Overwrite! {idx} buffer index for seq#{seq} is not empty')

        # If there's a possibility of events being lost or skipped # in the sequence,
        # a timer would be required to flush the buffer and avoid waiting for a missing event.
        self.buffer[idx] = seq, message

        return seq, idx

    def collect_sequence(self, from_idx):
        """Collect an ordered sequence of events starting from the given index.
        Removes events from the buffer.
        """
        idx = from_idx
        messages = []
        while idx in self.buffer:
            seq, message = self.buffer.pop(idx)
            idx = self.buffer_idx(seq + 1)
            self.last_seq = seq
            messages.append(message)

        return messages
