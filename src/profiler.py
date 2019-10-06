import time

class Profiler:
    batch_sizes = []
    batch_times = []
    processing_times = []
    last_triggered_time = 0
    last_profile_time = 0
    last_seq = 0

    def display(self, seq, queue):
        """Display profile information."""

        print('\n#' + '{:,}'.format(seq))
        print(f'queue_size: {queue.qsize()}')
        print(f'per_{seq - self.last_seq}\t{round(time.perf_counter() - self.last_profile_time, 4)}s')
        self.last_profile_time = time.perf_counter()
        self.last_seq = seq
