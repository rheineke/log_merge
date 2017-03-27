"""Read any number of files and write a single merged and ordered file"""
import sys
import time

from merge import merge_files

import fastparquet
import numpy as np
import pandas as pd


def generate_input_files(input_filenames, n, sort=True):
    for i_fn in input_filenames:
        df = _input_data_frame(n, sort=sort)
        df.to_csv(i_fn)


def _input_data_frame(n, max_interval=0):
    # Timestamp (int) index
    now = int(time.time())
    low = now - 1000
    high = now
    rel_time = np.random.randint(low=low, high=high, size=n)
    rel_time.sort()

    # Generate jitter in output: swap some times if < max_interval
    time_diff = np.diff(rel_time)
    time_diff = np.insert(time_diff, [0], sys.maxsize)
    diff_lt_lbl = time_diff < max_interval
    # TODO(rheineke): If true, calculate rand
    # TODO(rheineke): If rand > 0.5, swap with previous index

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
    with open(output_filename, 'w') as output_fp:
        merge_files(input_filenames, output_fp.write)
