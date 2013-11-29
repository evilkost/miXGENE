import os
import unittest

import pandas
import pandas.util.testing as tm

from workflow.structures import DataFrameStorage


class TestDataFrameStorage(unittest.TestCase):
    orig_filepath = "test/artifacts/es_assay.csv.gz"
    test_filepath = "test/tmp/test_df.csv.gz"

    #! Method name should start with `test` prefix
    def test_store_load(self):

        # source file - some expression set assay table
        orig_df = pandas.read_table(self.orig_filepath, sep=" ",
                                    compression="gzip", index_col=0)

        dfs = DataFrameStorage(self.test_filepath)
        dfs.store(orig_df)
        restored_df = dfs.load()

        # using custom assert function,
        #   because ==operator for DataFrame doesn't return boolean
        tm.assert_frame_equal(orig_df, restored_df)

    # another method to be used as test
    def test_load(self):
        dfs = DataFrameStorage(self.orig_filepath)
        df = dfs.load()

        # simple unittest assertions
        self.assertEqual(len(df.index), 22283)
        self.assertEqual(len(df.columns), 13)

    def tearDown(self):
        # clean up after each test done
        try:
            os.remove(self.test_filepath)
        except:
            pass
