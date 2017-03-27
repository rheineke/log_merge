import heapq
from contextlib import ExitStack


def merge_files(input_filenames, callback_func):
    with ExitStack() as stack:
        input_files = [stack.enter_context(open(fname, 'r')) for fname in input_filenames]

        # All opened files will automatically be closed at the end of
        # the with statement, even if attempts to open files later
        # in the list raise an exception

        _merge_files(input_files, callback_func)


def _merge_files(input_files, callback_func, max_interval=0):
    heap = []
    file_heaps = dict((f.fileno(), []) for f in input_files)

    header_line = '\n'
    for fp in input_files:
        header_line = fp.readline()  # Read header row

    # Initialize heap
    for fp in input_files:
        file_heap = file_heaps[fp.fileno()]
        _file_readlines(heap, fp, file_heap, max_interval)

    # Initialize output file header
    callback_func(header_line)

    while len(heap):
        lst_data = heapq.heappop(heap)
        fp = lst_data.fp
        file_heap = file_heaps[fp.fileno()]
        heapq.heappop(file_heap)

        _file_readlines(heap, fp, file_heap, max_interval)

        callback_func(str(lst_data))


def _file_readlines(heap, fp, file_heap, max_interval):
    line = fp.readline()
    # Push next line to heap if available
    while len(line):
        line = line.split(',')
        timestamp = int(line[0])
        data = OrderedData(timestamp, line[1:], fp)

        heapq.heappush(heap, data)
        heapq.heappush(file_heap, timestamp)

        if timestamp < file_heap[0] + max_interval:
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

if __name__ == '__main__':
    input_filenames = [
        'input/a.csv',
        'input/b.csv',
        'input/c.csv',
    ]
    output_filename = 'output/bar.csv'
    with open(output_filename, 'w') as output_fp:
        merge_files(input_filenames, output_fp.write)
