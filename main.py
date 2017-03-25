"""Read any number of files and write a single merged and ordered file"""
import time
from contextlib import ExitStack
import heapq

import fastparquet
import numpy as np
import pandas as pd


def merge_files(input_filenames, output_filename):
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

        hm = HeapManager(input_files, output_file.write, 0)
        hm.readwritelines()


class HeapManager:
    def __init__(self, input_files, callback_func, max_interval):
        self.input_files = input_files
        self.callback_func = callback_func
        self.max_interval = max_interval
        self.heap = []
        self.file_heaps = dict((f.fileno(), []) for f in input_files)

        header_line = '\n'
        for fp in input_files:
            header_line = fp.readline()  # Read header row

        # Initialize heap
        for fp in self.input_files:
            file_heap = self.file_heaps[fp.fileno()]

            # Initialize file's heap
            self._readlines(fp, file_heap)

        # Initialize output file header
        # TODO(rheineke): Improve this
        self.callback_func(header_line)

    def readwritelines(self):
        while len(self.heap):
            lst_data = heapq.heappop(self.heap)
            fp = lst_data.fp
            file_heap = self.file_heaps[fp.fileno()]
            heapq.heappop(file_heap)

            self._readlines(fp, file_heap)

            self.callback_func(str(lst_data))

    def _readlines(self, fp, file_heap):
        line = fp.readline()
        # Push next line to heap if available
        while len(line):
            line = line.split(',')
            timestamp = int(line[0])
            data = OrderedData(timestamp, line[1:], fp)

            heapq.heappush(self.heap, data)
            heapq.heappush(file_heap, timestamp)

            if timestamp < file_heap[0] + self.max_interval:
                break

            line = fp.readline()


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
    index = pd.Index(data=rel_time, name='timestamp')

    # Random data
    data = {
        'a': list(range(n))
    }

    return pd.DataFrame(data=data, index=index)


if __name__ == '__main__':
    input_filenames = [
        'input/a.csv',
        'input/b.csv',
        'input/c.csv',
    ]
    # generate_input_files(input_filenames, 15)
    output_filename = 'output/bar.csv'
    merge_files(input_filenames, output_filename)
