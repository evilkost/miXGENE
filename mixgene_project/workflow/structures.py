__author__ = 'kost'

import pandas as pd
import rpy2.robjects as R

class DataFrameStorage(object):
    def __init__(self, filepath):
        self.filepath = filepath

        self.sep = " "
        self.header = 0
        self.index_col = None
        self.compression = "gzip"

    def load(self):
        """
            @rtype  : pandas.DataFrame
            @return : Stored matrix
        """
        return pd.from_table(
            self.filepath,
            sep=self.sep,
            compression=self.compression,
            header=self.header,
            index_col=self.index_col,
        )

    def store(self, df):
        """
            @type   df: pandas.DataFrame
            @param  df: Stored matrix
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Given object isn't of DataFrame class: %s" % df)
        df.to_csv(
            self.filepath,
            sep=self.sep,
            compression=self.compression,
            header=self.header,
            index_col=self.index_col,
        )

    def to_r_obj(self):
        """
            @rtype  : R data frame
            @return : Stored matrix converted to R object
        """
        return R.r["read.table"](self.filepath, sep=self.sep, header=self.header)


class ExpressionSet(object):
    def __init__(self, base_dir, base_filename):
        """
            Expression data from micro array experiment.

            Actual matrices are stored in filesystem, so it's required
             to provide base_dir and base_base_filename.

            @type  base_dir: string
            @param base_dir: Path to directory where all data objects will be stored

            @type  base_filename: string
            @param base_filename: Basic name which is used as prefix for all stored data objects
        """
        self.base_dir = base_dir
        self.base_filename = base_filename

        self.assay_data_storage = None
        self.assay_metadata = {}

        self.pheno_data_storage = None
        self.pheno_metadata = {}

        self.annotation = None

        # Have no idea about 3 following variables
        self.feature_data = None
        self.experiment_data = None
        self.protocol_data = None

    def get_assay_data_frame(self):
        """
            @rtype: DataFrame
        """
        if self.assay_data_storage is None:
            raise RuntimeError("Assay data wasn't setup prior")
        return self.assay_data_storage.load()

    def store_assay_data_frame(self, df):
        """
            @type  df: DataFrame
            @param df: Table with expression data
        """
        if self.assay_data_storage is None:
            self.assay_data_storage = DataFrameStorage(
                filepath="%s\%s_assay.csv.gz" % (self.base_dir, self.base_filename))
        self.assay_data_storage.store(df)

    def get_pheno_data_frame(self):
        """
            @rtype: DataFrame
        """
        if self.pheno_data_storage is None:
            raise RuntimeError("Phenotype data wasn't setup prior")
        return self.pheno_data_storage.load()


    def store_pheno_data_frame(self, df):
        """
            @type df: DataFrame
        """
        if self.pheno_data_storage is None:
            self.pheno_data_storage = DataFrameStorage(
                filepath="%s\%s_pheno.csv.gz" % (self.base_dir, self.base_filename))
        self.pheno_data_storage.store(df)

    def to_r_obj(self):
        pass


class PlatformAnnotation(object):
    def __init__(self, platform_name, base_dir, base_filename):
        """
            Metadata about experiment platform

            Actual matrices are stored in filesystem, so it's required
             to provide base_dir and base_base_filename.

            @type  base_dir: string
            @param base_dir: Path to directory where all data objects will be stored

            @type  base_filename: string
            @param base_filename: Basic name which is used as prefix for all stored data objects
        """

        self.name = platform_name
        

