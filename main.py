import time

import pandas as pd


def write_input_data(filename):
    # TODO(rheineke): Finish this
    df = _input_data_frame(15)
    df.to_csv(filename)


def _input_data_frame(n):
    now = int(time.time())
    index = [now] * n
    df = pd.DataFrame(index=index)
    return df


def merge_files(input_filenames, output_filename):
    # TODO(rheineke): Finish this
    pass


if __name__ == '__main__':
    input_filenames = [
        'input/a.csv',
        'input/b.csv',
        'input/c.csv',
    ]
    output_filename = 'output/bar.csv'
    merge_files(input_filenames, output_filename)
