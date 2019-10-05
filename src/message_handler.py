import asyncio
import time
from collections import defaultdict
from src.profiler import Profiler

MAX_BUFFER_SIZE = 1000
profiler = Profiler()


class MessageHandler:
    # Ring buffer with the MAX_BUFFER_SIZE which restricts the amount of memory used.
    # Max size should be determined based on the distance between max arrived event and min unarrived = events needed to be buffered.
    ring_buffer = {}
    # User followers: {user_id -> [follower_id_1, follower_id_2]
    followers = defaultdict(list)
    # Currently connected users: {user_id -> writer, ...}
    clients = {}
    # Last processed event seq id.
    last_seq = 0
    # Max seq id in the buffer.
    max_seq = 0

    def add_client(self, user_id, writer):
        self.clients[user_id] = writer

    async def receive(self, event):
        seq, _ = event.split('|', maxsplit=1)
        seq = int(seq)

        if seq > 0 and seq % 100000 == 0:
            profiler.display(seq)

        self.ring_buffer[seq] = event
        self.max_seq = max(seq, self.max_seq)

        # Once a sequence from last_seq to max_seq is full, it can be processed one by one in order.
        if len(self.ring_buffer) == self.max_seq - self.last_seq:
            await self.process_buffer(self.last_seq + 1, self.max_seq)
            self.last_seq = self.max_seq
            self.ring_buffer = {}

    def ring_receive(self):
        #b_idx = buffer_idx(seq)
        # Check if an overwrite occurs in the buffer, meaning it's overflown and events are being lost.
        # if b_idx in ring_buffer:
        #    print(f'Overwrite! {b_idx} buffer index for seq#{seq} is not empty')
        # If there's a possibility of events being lost or skipped # in the sequence,
        # a timer would be required to flush the buffer and avoid waiting for a missing event.
        # todo: optimize ring buffer, probably a separate thred. then use b_idx
        # ring_buffer[b_idx] = event_msg
        # When a subsequent event is received, trigger the sequence processing
        # until next missing event seq is met.
        # todo:
        # if seq == last_seq + 1:
        #    print(f'Buffer filled: {len(ring_buffer)}')
        #    await process_buffer()
        pass

    async def process_buffer(self, from_seq, to_seq):
        """Iterate and process buffered events from seq to seq in correct order.
        Awaits for all sent messages tasks to complete.
        """
        processed = 0
        t = time.perf_counter()
        tasks = []
        for seq in range(from_seq, to_seq):
            processed += 1
            message = self.ring_buffer[seq]
            task = self.process_message(message)
            if task is list:
                tasks.extend(task)
            elif task is not None:
                tasks.append(task)

        await asyncio.gather(*tasks)
        profiler.store(t, processed)

    async def process_message(self, event):
        """Process an event.
        Return None, Task or Task[], where task is a user notification.

        Support five event types:
        - F: follow a user
        - U: unfollow a user
        - B: broadcast to all connected clients
        - P: private message from a user
        - S: user status update to all followers
        """
        seq, event_type, *ids = event.split('|')
        seq = int(seq)

        def task(id, msg):
            return asyncio.create_task(self.notify_user(id, msg))

        if event_type == 'F':
            #print(f'Follow from {ids[0]} to {ids[1]}')
            self.followers[ids[1]].append(ids[0])

            return asyncio.create_task(self.notify_user(ids[1], event))
        if event_type == 'U':
            # No notification
            #print(f'Unfollow from {ids[0]} to {ids[1]}')
            if ids[0] in self.followers[ids[1]]:
                self.followers[ids[1]].remove(ids[0])
            # else:
                # print(f'Error: User#{ids[0]} not following User#{ids[1]}')
            return
        if event_type == 'B':
            # print(f'Broadcast')
            return [task(user_id, event) for user_id in self.clients]
        if event_type == 'P':
            #print(f'Private message from {ids[0]} to {ids[1]}')
            return task(ids[1], event)
        if event_type == 'S':
            #print(f'Status Update from {ids[0]}')
            return [task(user_id, event) for user_id in self.followers[ids[0]]]

        raise Exception(f'Unknown event type "{event_type}"')

    async def notify_user(self, id, event):
        """Send event to the user.
        Client is disconnected if on read the connection fails.
        """
        if (id not in self.clients):
            return

        # It might be more effecient to process a whole events batch, and only then send a batch of messages (separated by \n) to users.
        # For now a simpler variant is used.
        writer = self.clients[id]
        try:
            # print(f"Send to User#{id}: {event}")
            # Drain BEFORE the write instead of AFTER, most likely it will return immediately as it was already drained while processing other clients
            await writer.drain()
            writer.write(event.encode())
        except (ConnectionError):
            print(f'User#{id} has disconnected')
            del self.clients[id]


def buffer_idx(seq):
    """Return circular buffer index from seq."""
    return seq % MAX_BUFFER_SIZE
