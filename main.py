import time
from contextlib import ExitStack
import heapq

import numpy as np
import pandas as pd


def merge_files(input_filenames, output_filename):
    # Initialize the heap
    heap = heapq.heapify([])

    # TODO: Consider opening with csv.DictReader()
    # TODO: Explore ways to open a binary file seamlessly with csv package
    with ExitStack() as stack:
        input_files = [stack.enter_context(open(fname, 'r')) for fname in input_filenames]

        num_uniq_filenos = len(set(f.fileno() for f in input_files))
        assert(num_uniq_filenos == len(input_files))

        output_file = stack.enter_context(open(output_filename, 'w'))
        # All opened files will automatically be closed at the end of
        # the with statement, even if attempts to open files later
        # in the list raise an exception
        for fp in input_files:
            fp.readline()  # Read header row, ignore for now

        hm = HeapManager(input_files, output_file)
        hm.readlines()


class HeapManager:
    def __init__(self, input_files, output_file):
        self.input_files = input_files
        self.output_file = output_file
        self.heap = []
        self.file_line_count = dict((f.fileno(), 0) for f in input_files)

        # TODO(rheineke): Handle files of different lengths
        # Initialize heap
        for fp in self.input_files:
            line = fp.readline()
            if not len(line):
                continue
            line = line.split(',')
            data = OrderedData(int(line[0]), line[1:], fp)

            heapq.heappush(self.heap, data)
            self.file_line_count[fp.fileno()] += 1

    def readlines(self):
        while len(self.heap):
            ordered_data = heapq.heappop(self.heap)

            # Push next line if available
            fp = ordered_data.fp
            next_line = fp.readline()
            if len(next_line):
                next_line = next_line.split(',')
                next_data = OrderedData(int(next_line[0]), next_line[1:], fp)
                heapq.heappush(self.heap, next_data)
            self.file_line_count[ordered_data.fp.fileno()] -= 1

            self.output_file.write(str(ordered_data))


class OrderedData:
    def __init__(self, timestamp, data, fp):
        self.timestamp = timestamp
        self.data = data
        self.fp = fp

    def __eq__(self, other):
        return self.timestamp == other.timestamp

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __str__(self):
        return ','.join([str(self.timestamp)] + self.data)


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
    # generate_input_files(input_filenames, 15)
    output_filename = 'output/bar.csv'
    merge_files(input_filenames, output_filename)
