import csv
import unittest

import numpy as np
import pandas as pd

from merge import merge_files
from main import generate_input_files, _input_data_frame


class InputFile(unittest.TestCase):
    def test_ordered_input_file(self):
        max_interval = 10
        df = _input_data_frame(100, max_interval=max_interval)
        self.assertFalse(df.index.is_monotonic_decreasing)
        # Not guaranteed to be false
        # self.assertFalse(df.index.is_monotonic_increasing)

        srs = pd.Series(df.index)
        diff_srs = srs.diff()
        # self.assertLessEqual(diff_srs.max(), max_interval)
        msg = '{}\n{}'.format(srs, diff_srs)
        self.assertGreaterEqual(diff_srs.min(), -max_interval, msg=msg)


class FileMerge(unittest.TestCase):
    def test_ordered_input_files(self):
        input_filenames = [
            'input/a.csv',
            'input/b.csv',
            'input/c.csv',
        ]
        generate_input_files(input_filenames, 15)

        for input_filename in input_filenames:
            self.assertTrue(input_filename)

        output_filename = 'output/bar.csv'
        with open(output_filename, 'w') as output_fp:
            merge_files(input_filenames, output_fp.write)

        self.assertTrue(output_filename)


def file_sorted_by_timestamp(filename):
    lst_timestamp = -np.inf
    with open(filename, 'r') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if lst_timestamp > row['timestamp']:
                return False
            lst_timestamp = row['timestamp']

    return True
