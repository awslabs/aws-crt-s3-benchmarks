import time


class PerfTimer:
    """Context manager that prints how long a `with` statement took"""

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            end = time.perf_counter()
            print(f"{self.name}: {end - self.start:.3f} sec")
