import asyncio
from src.profiler import Profiler
from src.event_handler import parse
import time

MAX_BUFFER_SIZE = 1000
profiler = Profiler()


class SourceServer:
    # Ring buffer with the MAX_BUFFER_SIZE which restricts the amount of memory used.
    # Max size should be determined based on the distance between max arrived event and min unarrived = events needed to be buffered.
    buffer = {}
    # Last processed event seq id.
    last_seq = 0
    # Max seq id in the buffer.
    max_seq = 0

    def __init__(self, sequence_cb, host='127.0.0.1', port=9090):
        self.sequence_cb = sequence_cb
        self.host = host
        self.port = port

    async def listen(self):
        server = await asyncio.start_server(self.connected_cb, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f"Event source: created server, {addr}")

        await server.serve_forever()

    async def connected_cb(self, reader, writer):
        print(f"Event source: connected")
        while True:
            message = await reader.readline()
            if not message:
                break
            seq, idx = self.receive(message.decode().rstrip())

            if seq > 0 and seq % 100000 == 0:
                profiler.display(seq)

            self.max_seq = max(self.max_seq, seq)

            # When a subsequent event is received, trigger the sequence processing
            # until next missing event seq is met.
            
            # This condition is for ring buffer.
            # if seq == self.last_seq + 1:
            if len(self.buffer) == self.max_seq - self.last_seq:
                sequence = self.collect_sequence(idx)
                self.last_seq = self.max_seq
                # asyncio.create_task(self.sequence_cb(sequence))
                await self.sequence_cb(sequence)
                self.buffer.clear()

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
            #seq, message = self.buffer.pop(idx)
            seq, message = self.buffer[idx]
            idx = buffer_idx(seq + 1)
            #self.last_seq = seq
            messages.append(message)

        return messages


# This if for ring buffer.
# def buffer_idx(seq): return seq % MAX_BUFFER_SIZE
def buffer_idx(seq): return seq
