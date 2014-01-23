import gzip

__author__ = 'kost'

import copy

import pandas as pd
import rpy2.robjects as R
from rpy2.robjects.packages import importr
from mixgene.settings import R_LIB_CUSTOM_PATH
import json


class DataFrameStorage(object):
    sep = " "
    header = 0
    index_col = 0
    compression = "gzip"

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self, nrows=None):
        """
            @type nrows: int or None
            @param nrows: Number of rows to read

            @rtype  : pandas.DataFrame
            @return : Stored matrix
        """
        return pd.read_table(
            self.filepath,
            sep=self.sep,
            compression=self.compression,
            header=self.header,
            index_col=self.index_col,
            nrows=nrows
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


class GenericStoreStructure(object):
    def __init__(self, base_dir, base_filename, *args, **kwargs):
        self.base_dir = base_dir
        self.base_filename = base_filename

    def form_filepath(self, suffix):
        return "%s/%s_%s.csv.gz" % (self.base_dir, self.base_filename, suffix)


class PcaResult(GenericStoreStructure):
    def __init__(self, base_dir, base_filename):
        super(PcaResult, self).__init__(base_dir, base_filename)
        self.pca_storage = None

    def store_pca(self, df):
        if self.pca_storage is None:
            self.pca_storage = DataFrameStorage(self.form_filepath("pca"))
        self.pca_storage.store(df)

    def get_pca(self):
        if self.pca_storage is None:
            raise RuntimeError("PCA data wasn't stored prior")
        return self.pca_storage.load()


class ExpressionSet(GenericStoreStructure):
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
        super(ExpressionSet, self).__init__(base_dir, base_filename)

        self.assay_data_storage = None
        self.assay_metadata = {}

        self.pheno_data_storage = None
        self.pheno_metadata = {}

        self.annotation = None

        # Have no idea about 3 following variables
        self.feature_data = None
        self.experiment_data = None
        self.protocol_data = None

    def clone(self, base_filename, clone_data_frames=False):
        es = ExpressionSet(self.base_dir, base_filename)

        es.annotation = self.annotation
        es.feature_data = self.feature_data
        es.experiment_data = self.experiment_data
        es.protocol_data = self.protocol_data

        es.assay_metadata = self.assay_metadata
        es.pheno_metadata = self.pheno_metadata
        if clone_data_frames:
            es.store_assay_data_frame(self.get_assay_data_frame())
            es.store_pheno_data_frame(self.get_pheno_data_frame())

        return es

    def get_assay_data_frame(self):
        """
            @rtype: pd.DataFrame
        """
        if self.assay_data_storage is None:
            raise RuntimeError("Assay data wasn't setup prior")
        return self.assay_data_storage.load()

    def store_assay_data_frame(self, df):
        """
            @type  df: pd.DataFrame
            @param df: Table with expression data
        """
        if self.assay_data_storage is None:
            self.assay_data_storage = DataFrameStorage(
                filepath="%s/%s_assay.csv.gz" % (self.base_dir, self.base_filename))
        self.assay_data_storage.store(df)

    def get_pheno_data_frame(self):
        """
            @rtype: pd.DataFrame
        """
        if self.pheno_data_storage is None:
            raise RuntimeError("Phenotype data wasn't setup prior")
        return self.pheno_data_storage.load()

    def store_pheno_data_frame(self, df):
        """
            @type df: pd.DataFrame
        """
        if self.pheno_data_storage is None:
            self.pheno_data_storage = DataFrameStorage(
                filepath="%s/%s_pheno.csv.gz" % (self.base_dir, self.base_filename))
        self.pheno_data_storage.store(df)

    def to_r_obj(self):
        pass

    def get_pheno_column_as_r_obj(self, column_name):
        pheno_df = self.get_pheno_data_frame()
        column = pheno_df[column_name].tolist()
        return R.r['factor'](R.StrVector(column))

    def to_json_preview(self, row_number=20):
        assay_df = self.assay_data_storage.load(row_number)
        pheno_df = self.pheno_data_storage.load(row_number)

        result = {
            "assay_metadata": self.assay_metadata,
            "assay": json.loads(assay_df.to_json(orient="split")),
            "pheno_metadata": self.pheno_metadata,
            "pheno": json.loads(pheno_df.to_json(orient="split")),
        }
        return json.dumps(result)


class GeneSets(object):
    def __init__(self, description=None, genes=None):
        if description is not None:
            self.description = description
        else:
            self.description = {}

        if genes is not None:
            self.genes = genes
        else:
            self.genes = {}

        self.org = ""
        self.units = ""

    def to_r_obj(self):
        gene_sets = R.ListVector(dict([
            (k, R.StrVector(list(v)))
            for k, v in self.genes.iteritems()
            if len(v)
        ]))
        return gene_sets


        # mgs = R.r['new']('mixGeneSets')
        # mgs.do_slot_assign("gene.sets", gene_sets)
        # mgs.do_slot_assign("org", R.StrVector([self.org]))
        # mgs.do_slot_assign("units", R.StrVector([self.units]))
        # return mgs

class GmtStorage(object):
    def __init__(self, filepath, compression=None, sep=None):
        """

        @param sep:
        @type filepath: str
        @param filepath: absolute path to stored object

        @param compression: either None of "gzip"

        @param sep: elements separator, default  \t

        @rtype: GeneSets
        @return:
        """
        self.filepath = filepath
        self.compression = compression
        if sep is not None:
            self.sep = sep
        else:
            self.sep = "\t"

    def load(self):
        """
            @rtype  : GeneSets
            @return : Gene sets
        """
        gene_sets = GeneSets(dict(), dict())

        def read_inp(inp):
            for line in inp:
                split = line.strip().split(self.sep)
                if len(split) < 3:
                    continue
                key = split[0]
                gene_sets.description[key] = split[1]
                gene_sets.genes[key] = split[2:]
        if self.compression == "gzip":
            with gzip.open(self.filepath) as inp:
                read_inp(inp)
        else:
            with open(self.filepath) as inp:
                read_inp(inp)

        return gene_sets

    def store(self, gene_sets):
        """
            @type gene_sets: GeneSets
            @param gene_sets: Gene sets

            @return: None
        """
        def write_out(out):
            for key in gene_sets.description.keys():
                description = gene_sets.description[key]
                elements = gene_sets.genes[key]
                out.write("%s\t%s\t%s\n" % (
                    (key, description, "\t".join(elements))
                ))
        if self.compression == "gzip":
            with gzip.open(self.filepath, "w") as output:
                write_out(output)
        else:
            with open(self.filepath, "w") as output:
                write_out(output)


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
        """
            @rtype: GmtStorage
        """
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
                filepath="%s/%s.gmt.gz" % (self.base_dir, self.base_filename),
                compression="gzip"
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


class SequenceContainer(object):
    def __init__(self, fields, sequence=None):
        self.sequence = sequence or []
        self.fields = fields
        self.iterator = -1

    def is_end(self):
        if self.iterator == len(self.sequence) - 1:
            return True
        return False

    def append(self, element):
        self.sequence.append(element)

    def apply_next(self):
        """
            Set block properties from the current sequence element

            @type block: workflow.Block

            @return: None
            @throws: StopIteration
        """
        self.iterator += 1
        if self.iterator >= len(self.sequence):
            raise StopIteration()

        # el = self.sequence[self.iterator]
        # for field in self.fields:
        #     setattr(block, field, getattr(el, field))

    def get_field(self, field):
        return self.sequence[self.iterator][field]

    def reset_iterator(self):
        self.iterator = -1


class IntegerValue(object):
    def __init__(self, val):
        self.val = val
