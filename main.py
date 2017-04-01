"""Read any number of files and write a single merged and ordered file"""
import sys
import time

import fastparquet
import numpy as np
import pandas as pd


def generate_input_files(input_filenames, n, max_interval=0):
    for i_fn in input_filenames:
        df = _input_data_frame(n, max_interval=max_interval)
        df.to_csv(i_fn)


def _input_data_frame(n, max_interval):
    ts = 'timestamp'

    # Timestamp (int) index
    now = int(time.time())
    low = now - 1000
    high = now
    rel_time = np.random.randint(low=low, high=high, size=n)
    rel_time.sort()
    time_srs = pd.Series(data=rel_time, name=ts)
    sorted_time_srs = time_srs.sort_values()

    # Generate jitter in output: swap some times if < max_interval
    # Do not swap consecutive pairs
    one_diff_srs = sorted_time_srs.diff(periods=1)
    two_diff_srs = sorted_time_srs.diff(periods=2)
    # Time difference less than max_interval
    diff_lt_lbl = (one_diff_srs < max_interval) & (two_diff_srs < max_interval)

    # Do not swap consecutive pairs
    swap_lbl = np.random.rand(n) >= 0.5
    lst_nonswap_lbl = ~np.roll(swap_lbl, shift=1)
    nonconsec_swap_lbl = swap_lbl & lst_nonswap_lbl
    # Randomly choose swaps among time difference less than max_interval
    swap_diff_lt_lbl = nonconsec_swap_lbl & diff_lt_lbl
    # Swap
    for i, swap in enumerate(swap_diff_lt_lbl):
        if swap:
            sorted_time_srs[i-1], sorted_time_srs[i] = sorted_time_srs[i], sorted_time_srs[i-1]

    index = pd.Index(data=sorted_time_srs.values, name=ts)

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

    df = _input_data_frame(100, max_interval=10)
