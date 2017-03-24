import csv
import unittest

import numpy as np

from main import generate_input_files, merge_files


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
        merge_files(input_filenames, output_filename)

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
