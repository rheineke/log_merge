import time
from contextlib import ExitStack
import heapq

import numpy as np
import pandas as pd


def merge_files(input_filenames, output_filename):
    heap = heapq.heapify([])

    # TODO: Consider opening with csv.DictReader()
    # TODO: Explore ways to open a binary file seamlessly with csv package
    with ExitStack() as stack:
        input_files = [stack.enter_context(open(fname, 'r')) for fname in input_filenames]
        output_file = stack.enter_context(open(output_filename, 'w'))
        # All opened files will automatically be closed at the end of
        # the with statement, even if attempts to open files later
        # in the list raise an exception
        # TODO(rheineke): Finish this

        # Initialize the heap
        for fp in input_files:
            fp.readline()  # Read header row, ignore for now

        for fp in input_files:
            line = fp.readline().split(',')
            # TODO: Need an object that is ordered by timestamp, stores the rest
            # of the line and points back to correct file handle for next line
            heapq.heappush(heap, int(line[0]))


def generate_input_files(input_filenames, n, sort=True):
    for i_fn in input_filenames:
        df = _input_data_frame(n, sort=sort)
        df.to_csv(i_fn)


def _input_data_frame(n, sort=False):
    # Timestamp (int) index
    now = int(time.time())
    low = now - 1000
    high = now
    rel_time = np.random.randint(low=low, high=high, size=n)
    if sort:
        rel_time.sort()

    # Random data
    data = {
        'a': list(range(n))
    }

    return pd.DataFrame(data=data, index=rel_time)


if __name__ == '__main__':
    input_filenames = [
        'input/a.csv',
        'input/b.csv',
        'input/c.csv',
    ]
    generate_input_files(input_filenames, 15)
    output_filename = 'output/bar.csv'
    merge_files(input_filenames, output_filename)
