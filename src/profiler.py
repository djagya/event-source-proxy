import time


class Profiler:
    batch_sizes = []
    batch_times = []
    processing_times = []
    last_triggered_time = 0
    last_profile_time = 0
    last_seq = 0

    def display(self, seq):
        """Display profile information."""

        def avg(x): return sum(x) / len(x)

        print('\n#' + '{:,}'.format(seq))
        print(f'per_{seq - self.last_seq}\t{round(time.perf_counter() - self.last_profile_time, 4)}s\n'
              f'avg_batch_size     \t{round(avg(self.batch_sizes))}\n'
              f'avg_time_between   \t{round(avg(self.batch_times), 4)}s\n'
              f'avg_processing_time\t{round(avg(self.processing_times), 4)}s')
        self.last_profile_time = time.perf_counter()
        self.last_seq = seq

    def store(self, start_time, processed_n):
        """Store profile information."""

        global last_triggered_time
        self.batch_times.append(time.perf_counter() - self.last_triggered_time)
        last_triggered_time = time.perf_counter()
        self.batch_sizes.append(processed_n)
        self.processing_times.append(time.perf_counter() - start_time)
