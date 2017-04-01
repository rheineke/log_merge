"""Read any number of files and write a single merged and ordered file"""
import time

import fastparquet
import numpy as np
import pandas as pd


def generate_input_files(input_filenames, n, max_interval=0):
    for i_fn in input_filenames:
        df = _input_data_frame(n,
                               max_interval=max_interval,
                               relative_time_period=1000)
        df.to_csv(i_fn)


def _input_data_frame(n, max_interval, relative_time_period):
    """

    :param n: Number of timestamp entries
    :param max_interval: Maximum time difference between non-monotonically 
    increasing entries
    :param relative_time_period: Maximum time period between first and last 
    timestamp entries
    :return: 
    """
    ts = 'timestamp'

    # Timestamp (int) index
    now = int(time.time())
    low = now - relative_time_period
    high = now
    rel_time = np.random.randint(low=low, high=high, size=n)
    rel_time.sort()

    # Generate jitter in output: swap some times if < max_interval
    # Do not swap consecutive pairs
    one_diff = np.diff(rel_time, n=1)
    one_diff = np.insert(one_diff, [0], max_interval)

    two_diff = np.diff(rel_time, n=2)
    two_diff = np.concatenate(([max_interval, max_interval], two_diff))
    # Time difference less than max_interval
    diff_lt_lbl = (one_diff < max_interval) & (two_diff < max_interval)

    # Do not swap consecutive pairs
    swap_lbl = np.random.rand(n) >= 0.5
    lst_nonswap_lbl = ~np.roll(swap_lbl, shift=1)
    nonconsec_swap_lbl = swap_lbl & lst_nonswap_lbl
    # Randomly choose swaps among time difference less than max_interval
    swap_diff_lt_lbl = nonconsec_swap_lbl & diff_lt_lbl
    # Swap
    for i, swap in enumerate(swap_diff_lt_lbl):
        if swap:
            rel_time[i-1], rel_time[i] = rel_time[i], rel_time[i-1]

    index = pd.Index(data=rel_time, name=ts)

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
    # generate_input_files(input_filenames, n=15, max_interval=15)
    # output_filename = 'output/bar.csv'
    # with open(output_filename, 'w') as output_fp:
    #     merge_files(input_filenames, output_fp.write)

    df = _input_data_frame(100, max_interval=10, relative_time_period=1000)
