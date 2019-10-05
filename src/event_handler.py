import asyncio
import time
from collections import defaultdict
from src.profiler import Profiler

def parse(event):
        return event.split('|')

class EventHandler:
    # User followers: {user_id -> [follower_id_1, follower_id_2]
    followers = defaultdict(list)

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
        seq, event_type, *ids = event.split('|')
        seq = int(seq)

        if event_type == 'F':
            #print(f'Follow from {ids[0]} to {ids[1]}')
            self.followers[ids[1]].append(ids[0])
            return [ids[1]]
        if event_type == 'U':
            # No notification
            #print(f'Unfollow from {ids[0]} to {ids[1]}')
            if ids[0] in self.followers[ids[1]]:
                self.followers[ids[1]].remove(ids[0])
            # else:
                # print(f'Error: User#{ids[0]} not following User#{ids[1]}')
            return []
        if event_type == 'B':
            # print(f'Broadcast')
            return True
        if event_type == 'P':
            #print(f'Private message from {ids[0]} to {ids[1]}')
            return [ids[1]]
        if event_type == 'S':
            #print(f'Status Update from {ids[0]}')
            return self.followers[ids[0]]

        raise Exception(f'Unknown event type "{event_type}"')
