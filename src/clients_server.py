import asyncio
import time
from collections import defaultdict
import src.vars

clients = {}
buffer = {}
last_processed_seq_n = 0
max_buffer_seq_n = 0
last_triggered = 0

batch_sizes = []
batch_times = []
processing_times = []

# user_id -> [user_id1, user_id2]
followers = defaultdict(list)

async def handle_event(event_msg):
    # todo: rework to a class
    global last_processed_seq_n
    global max_buffer_seq_n
    global buffer
    global last_triggered

    seq_id, _ = event_msg.split('|', maxsplit=1)
    seq_id = int(seq_id)

    # The ordering strategy depends on the requirements:
    # - do we need to notify clients as soon as possible
    # - or is it allowed to have a delay (e.g. a second), to buffer events and ensure the correct order

    # Ordering can be done on per-user base (i.e. a bucket of events for each user with independent timers),
    # but since there are events directed to multiple user at once (broadcast, status update)
    # it's much easier in terms of this task to handle ordering on a lower level buffering the main events stream before processing.

    buffer[seq_id] = event_msg

    # Update max buffer sequence number
    if seq_id > max_buffer_seq_n:
        max_buffer_seq_n = seq_id

    if seq_id > 0 and seq_id % 100000 == 0:
        print('\n#' + '{:,}'.format(seq_id))
        print(f'avg_batch_size     \t{round(sum(batch_sizes)/len(batch_sizes))}')
        print(f'avg_time_between   \t{round(sum(batch_times)/len(batch_times), 4)}s')
        print(f'avg_processing_time\t{round(sum(processing_times)/len(processing_times), 4)}s')

    # Trigger processing when buffer contains exactly the same amounf of events as diff between the last processed seq_n and max buffer seq_n.
    # In this case all events between these numbers will be in buffer and can be sent in correct order.
    # last = 3, max = 10; buffer must contain 10-3=7 events: from #4 to #10
    if max_buffer_seq_n - last_processed_seq_n == len(buffer):
        #print(f'Processing: since_last={time.perf_counter() - last_triggered}, '
        #      f'last_seq={last_processed_seq_n}, max_seq={max_buffer_seq_n}')
        batch_times.append(time.perf_counter() - last_triggered)
        last_triggered = time.perf_counter()
        batch_sizes.append(len(buffer))

        # todo: right now there's no timeout, so it might take a long time until a full sequence of events is buffered
        # todo: maybe better process sub-sequences instead of waiting for the full sequence. but how to check efficient if a sequence doesn't have skips?

        # Trigger processing from last_seq_n + 1 to max_seq_n to avoid unnecessary soring and instead accessing the hashmap by key.
        # todo: for now it's a blocking task, so source_server won't be accepting new messages until the buffer is processed
        t = time.perf_counter()
        await process_buffer(last_processed_seq_n + 1, max_buffer_seq_n)
        processing_times.append(time.perf_counter() - t)
        #print(f'Took {time.perf_counter() - t} to process')
        last_processed_seq_n = max_buffer_seq_n
        buffer = {}


async def process_buffer(from_n, to_n):
    global followers
    # todo: better split a message just once and store that.

    tasks = []
    for i in range(from_n, to_n):
        event_msg = buffer[i]
        seq_id, event_type, *ids = event_msg.split('|')
        seq_id = int(seq_id)

        if event_type == 'F':
            #print(f'Follow from {ids[0]} to {ids[1]}')
            followers[ids[1]].append(ids[0])

            tasks.append(asyncio.create_task(notify_user(ids[1], event_msg)))
        elif event_type == 'U':
            # No notification
            #print(f'Unfollow from {ids[0]} to {ids[1]}')
            if ids[0] not in followers[ids[1]]:
                #print(f'Error: User#{ids[0]} not following User#{ids[1]}')
                continue

            followers[ids[1]].remove(ids[0])
        elif event_type == 'B':
            #print(f'Broadcast')

            for user_id in clients:
                tasks.append(asyncio.create_task(
                    notify_user(user_id, event_msg)))
        elif event_type == 'P':
            #print(f'Private message from {ids[0]} to {ids[1]}')
            tasks.append(asyncio.create_task(notify_user(ids[1], event_msg)))
        elif event_type == 'S':
            #print(f'Status Update from {ids[0]}')

            for user_id in followers[ids[0]]:
                tasks.append(asyncio.create_task(
                    notify_user(user_id, event_msg)))

    await asyncio.gather(*tasks)


async def notify_user(id, event):
    """Send event to the user.

    Args:
    id -- user id
    event -- exact event message to send
    """

    # It can happen that client will connect at this exact moment of processing, but this edge-case is not handled for simplicity
    if (id not in clients):
        return

    writer = clients[id]

    # It might be more effecient to process a whole events batch, and only then send a batch of messages (separated by \n) to users.
    # For now a simpler variant is used.
    try:
        #print(f"Send to User#{id}: {event}")

        # Drain BEFORE the write instead of AFTER, most likely it will return immediately as it was already drained while processing other clients
        await writer.drain()
        writer.write(event.encode())

    except (ConnectionError):
        print(f'User#{id} has disconnected')


async def listen():
    async def connection_handler(reader, writer):
        # Once connected, wait for the id received as a first client message
        user_id = (await reader.readline()).decode().rstrip()
        clients[user_id] = writer

        print(f"Clients: connected user_id={user_id}")

    server = await asyncio.start_server(connection_handler, '127.0.0.1', 9099)
    addr = server.sockets[0].getsockname()
    print(f"Clients: created server, {addr}")

    return server
