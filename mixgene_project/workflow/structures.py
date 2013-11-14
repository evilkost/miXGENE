import gzip

__author__ = 'kost'

import pandas as pd
import rpy2.robjects as R
from rpy2.robjects.packages import importr
from mixgene.settings import R_LIB_CUSTOM_PATH


class DataFrameStorage(object):
    sep = " "
    header = 0
    index_col = 0
    compression = "gzip"

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
            @rtype  : pandas.DataFrame
            @return : Stored matrix
        """
        return pd.read_table(
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
        with gzip.open(self.filepath, "wb") as output:
            df.to_csv(
                output,
                sep=self.sep,
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
                filepath="%s/%s_assay.csv.gz" % (self.base_dir, self.base_filename))
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
                filepath="%s/%s_pheno.csv.gz" % (self.base_dir, self.base_filename))
        self.pheno_data_storage.store(df)

    def to_r_obj(self):
        pass


class GmtStorage(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.compression = "gzip"

    def load(self):
        """
            @rtype  : dict
            @return : {Set Name -> (Set description, [Set elements])}
        """
        gene_sets = {}
        with gzip.open(self.filepath) as inp:
            for line in inp:
                split = line.strip().split("\t")
                if len(split) < 3:
                    continue
                key = split[0]
                gene_sets[key] = (split[1], split[2:])

        return gene_sets

    def store(self, gene_sets):
        with gzip.open(self.filepath, "w") as output:
            for key, rest in gene_sets.iteritems():
                description, elements = rest
                output.write("%s\t%s\t%s\n" % (
                    (key, description, "\t".join(elements))
                ))


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
        self.base_dir = base_dir
        self.base_filename = base_filename

        self.name = platform_name
        self.gmt_storage = None

        self.gene_units = None
        self.set_units = None

    def get_gmt(self):
        if self.gmt_storage is None:
            raise RuntimeError("Gmt wasn't setup prior")
        return self.gmt_storage.load()

    def store_gmt(self, gene_sets):
        """
            @type  gene_sets: dict
            @param gene_sets: {Set Name -> (Set description, [Set elements])}
        """
        if self.gmt_storage is None:
            self.gmt_storage = GmtStorage(
                filepath="%s\%s.gmt.gz" % (self.base_dir, self.base_filename)
            )
        self.gmt_storage.store(gene_sets)

    def to_r_obj(self):
        importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
        gene_sets = self.load_gmt()
        r_gene_sets = R.ListVector(dict([
            (k, R.StrVector(descr_and_elements[1]))
            for k, descr_and_elements in gene_sets.iteritems()
        ]))

        mgs = R.r['new']('mixGeneSets')
        mgs.do_slot_assign("gene.sets", r_gene_sets)
        mgs.do_slot_assign("org", R.StrVector([self.org]))
        mgs.do_slot_assign("units", R.StrVector([self.units]))

        return mgs
