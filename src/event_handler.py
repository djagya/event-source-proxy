import asyncio
import time
from collections import defaultdict
from src.profiler import Profiler

def parse(event):
    pieces = event.split('|')
    if len(pieces) < 2:
        raise Exception('Invalid event message')
    seq, e_type, *ids = pieces
    return [int(seq), e_type] + [int(id) for id in ids]

class EventHandler:
    # User followers: {user_id -> [follower_id_1, follower_id_2]
    followers = defaultdict(set)

    def process(self, event):
        """Process an event.
        Return a list of user ids which should be notified with the given event.
        If True returned, all connected users should be notified.

        Support five event types:
        - F: follow a user
        - U: unfollow a user
        - B: broadcast to all connected clients
        - P: private message from a user
        - S: user status update to all followers
        """
        _, event_type, *ids = parse(event)

        if event_type == 'F':
            # Follow from ids[0] to ids[1]
            self.followers[ids[1]].add(ids[0])
            return [ids[1]]
        if event_type == 'U':
            # Unfollow from ids[0] to ids[1]}. No notification
            if ids[0] in self.followers[ids[1]]:
                self.followers[ids[1]].discard(ids[0])
            return []
        if event_type == 'B':
            # Broadcast
            return True
        if event_type == 'P':
            # Private message from ids[0] to ids[1]}
            return [ids[1]]
        if event_type == 'S':
            # Status Update from ids[0]
            return list(self.followers[ids[0]])

        raise Exception(f'Unknown event type "{event_type}"')
