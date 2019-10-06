# Solution for the developer challenge "Follower Maze" by Soundcloud

This is the implementation of a proxy between a source server and many clients.

Python v3.7 was used for this purpose, but I have to say: I don't have a lof of experience with Python (as opposed to PHP – but I want to migrate from it), so some solutions might appear unconvential.

At first I tried to implement this based on `async`/`await` and event loop, but it proved to be too slow to handle the event stream making the buffer overfill and throttling the event source connection.

So now the implementation based on regular sockets and threads. There are two servers running is separate threads and additionally one queue worker to process received events.

To profile the code `cProfile` and `snakeviz` were used:
```bash
$ python -m cProfile -o out.profile main.py
$ snakeviz out.profile
```

## Starting the service

```bash
$ python main.py [--buffer=10000]
```

Where **`--buffer`** specifies the max queue and buffer size, and should be determined based on the max allowed out-of-order events: the diff between the max arrived and min unarrived event sequence numbers.

## `ClientsServer`

This class listens for connecting clients.

It provides `ClientsServer.notify` method to forward an event to a list of users. When a write to the socket fails, a client is considered disconnected.

## `SourceServer`

This class accepts a connection from the event source.

To handle out-of-order events, a ring buffer is used as a conventional data structure to handle a data stream.
Considering there are no lost events and skipped sequence numbers, it waits for an event with the next after last sequence number to appear – then an ordered sub-sequnce from `last_seq` up to the next empty cell in the buffer can be enqued for processing.

## `EventHandler`

This class implements the business logic of processing different event types specified in `instructions.md`.
