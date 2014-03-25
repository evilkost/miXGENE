import gzip
import cPickle as pickle
from uuid import uuid1
import pandas as pd
import rpy2.robjects as R
from copy import deepcopy

import json

from workflow.input import AbsInputVar
from wrappers.scoring import metrics


class PickleStorage(object):
    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        return pickle.loads(gzip.open(self.filepath, "rb").read())

    def store(self, obj):
        with gzip.open(self.filepath, "wb") as out:
            pickle.dump(obj, out, 2)


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
        if self.compression == "gzip":
            with gzip.open(self.filepath, "wb") as output:
                df.to_csv(
                    output,
                    sep=self.sep,
                    index_col=self.index_col,
                )
        elif self.compression is None:
            with open(self.filepath, "wb") as output:
                df.to_csv(
                    output,
                    sep=self.sep,
                    index_col=self.index_col,
                )

    # def to_r_obj(self):
    #     """
    #         @rtype  : R data frame
    #         @return : Stored matrix converted to R object
    #     """
    #     return R.r["read.table"](self.filepath, sep=self.sep, header=self.header)


class GenericStoreStructure(object):
    def __init__(self, base_dir, base_filename, *args, **kwargs):
        self.base_dir = base_dir
        self.base_filename = base_filename

    def form_filepath(self, suffix):
        return "%s/%s_.%s.gz" % (self.base_dir, self.base_filename, suffix)


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


class BinaryInteraction(GenericStoreStructure):
    def __init__(self, *args, **kwargs):
        super(BinaryInteraction, self).__init__(*args, **kwargs)
        self.storage = None
        self.row_units = ""
        self.col_units = ""

    def store_matrix(self, df):
        if self.storage is None:
            self.storage = DataFrameStorage(self.form_filepath("interaction"))
        self.storage.store(df)

    def load_matrix(self):
        if self.storage is None:
            raise RuntimeError("Interaction data wasn't stored prior")
        return self.storage.load()


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
        self.pheno_metadata = {
            "user_class_title": "User_class"
        }

        self.working_unit = None
        self.annotation = None

        # Have no idea about 3 following variables
        self.feature_data = None
        self.experiment_data = None
        self.protocol_data = None

    def __str__(self):
        return "ExpressionSet, pheno: %s , assay: %s" % (self.pheno_data_storage, self.assay_data_storage)

    def clone(self, base_filename, clone_data_frames=False):
        es = ExpressionSet(self.base_dir, base_filename)

        es.working_unit = deepcopy(self.working_unit)
        es.annotation = deepcopy(self.annotation)
        es.feature_data = deepcopy(self.feature_data)
        es.experiment_data = deepcopy(self.experiment_data)
        es.protocol_data = deepcopy(self.protocol_data)

        es.assay_metadata = deepcopy(self.assay_metadata)
        es.pheno_metadata = deepcopy(self.pheno_metadata)
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


class GS(object):
    def __init__(self, description=None, genes=None):
        if description is not None:
            self.description = description
        else:
            self.description = {}

        if genes is not None:
            self.genes = genes
        else:
            self.genes = {}

    def to_r_obj(self):
        gene_sets = R.ListVector(dict([
            (k, R.StrVector(list(v)))
            for k, v in self.genes.iteritems()
            if len(v)
        ]))
        return gene_sets


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
            @rtype  : GS
        """
        gene_sets = GS(dict(), dict())

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
            @type gene_sets: GS
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


class GeneSets(GenericStoreStructure):
    def __init__(self, base_dir, base_filename):
        super(GeneSets, self).__init__(base_dir, base_filename)

        self.storage = None
        self.metadata = {
            "org": list(),
            "set_units": str(),
            "gene_units": str(),
        }

    def store_gs(self, gs):
        """
            @type gs: GS
        """
        if self.storage is None:
            self.storage = GmtStorage(
                filepath="%s/%s_gene_sets.gmt.gz" % (self.base_dir, self.base_filename),
                compression="gzip"
            )
        self.storage.store(gs)

    def get_gs(self):
        if self.storage is None:
            raise RuntimeError("No gene sets was stored")
        return self.storage.load()


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
        self.gene_sets = GeneSets(base_dir, "%s_platform" % base_filename)


class SequenceContainer(object):
    def __init__(self, fields=None, sequence=None):
        self.sequence = sequence or []
        self.fields = fields or {}  # TODO: Just names, or contains some meta info ?
        self.iterator = -1

    def is_end(self):
        if self.iterator == len(self.sequence) - 1:
            return True
        return False

    def append(self, element):
        self.sequence.append(element)

    def clean_content(self):
        self.sequence = []
        self.iterator = -1

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

    def to_dict(self):
        dict_seq = []
        for cell in self.sequence:
            if cell is not None and self.fields is not None:
                cell_dict = {}
                for field in self.fields.keys():
                    obj = cell.get(field)
                    if hasattr(obj, "to_dict"):
                        cell_dict[field] = obj.to_dict()
                    else:
                        cell_dict[field] = str(obj)
                dict_seq.append(cell_dict)
            else:
                dict_seq.append(None)
        return {
            "fields": self.fields,
            "seq": dict_seq
        }


class ClassifierResult(GenericStoreStructure):
    def __init__(self, *args, **kwargs):
        super(ClassifierResult, self).__init__(*args, **kwargs)

        self.classifier = ""
        self.labels_encode_vector = []
        self.scores = {}  # metric -> value
        self.y_true = []
        self.y_predicted = []
        self.model_storage = None

    def get_model(self):
        if self.model_storage is None:
            raise RuntimeError("Model wasn't stored")
        return self.model_storage.load()

    def store_model(self, model):
        if self.model_storage is None:
            self.model_storage = PickleStorage(self.form_filepath("classifier_result"))
        self.model_storage.store(model)

    def to_dict(self):
        scores_dict = {}
        for metric in metrics:
            if metric.name in self.scores:
                scores_dict[metric.name] =\
                    metric.to_dict(self.scores[metric.name])

        return {
            "classifier": self.classifier,
            "scores": scores_dict,
        }


class TableResult(GenericStoreStructure):
    def __init__(self, base_dir, base_filename):
        super(TableResult, self).__init__(base_dir, base_filename)

        self.table_storage = None
        self.metadata = dict()
        self.headers = []

    def to_dict(self):
        # df = self.get_table()
        return {
            "headers": self.headers
        }

    def store_table(self, df):
        if self.table_storage is None:
            self.table_storage = DataFrameStorage(self.form_filepath("result_table"))
        self.table_storage.store(df)

    def get_table(self):
        """
            @rtype: pd.DataFrame
        """
        if self.table_storage is None:
            raise RuntimeError("Result table data wasn't stored prior")
        return self.table_storage.load()


### R Legacy
class mixML(object):
    def __init__(self, exp, rMixML, csv_filename):
        self.uuid = str(uuid1())
        self.template = "workflow/result/mixML.html"
        self.title = "mixML"

        self.model = str(rMixML.do_slot('model')[0])
        self.acc = int(rMixML.do_slot('acc')[0])
        self.working_units = list(rMixML.do_slot('working.units'))

        predicted = rMixML.do_slot('predicted')

        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)

        R.r['write.table'](predicted, self.filepath, row_names=True, col_names=True)

        self.has_col_names = True
        self.has_row_names = True
        self.csv_delimiter = " "


class FileInputVar(AbsInputVar):
    #TODO: rework into storage + variable
    def __init__(self, *args, **kwargs):
        super(FileInputVar, self).__init__(*args, **kwargs)
        self.input_type = "file"
        self.is_done = False
        self.is_being_fetched = False

        self.file_type = None
        self.filename = None
        self.filepath = None
        self.file_extension = "csv"
        self.is_gzipped = False

        self.file_format = None

        self.geo_uid = None
        self.geo_type = None

    def to_dict(self, *args, **kwargs):
        return self.__dict__

    def set_file_type(self, file_type):
        if file_type in ['user', 'ncbi_geo', 'gmt']:
            self.file_type = file_type
        else:
            raise Exception("file type should be in [`user`, `ncbi_geo`, `gmt`], not %s" % type)

import hashlib
m = hashlib.md5()


def fix_val(value):
    if not pd.notnull(value):
        return None
    return str(value)

def prepare_phenotype_for_js_from_es(es):
    """
        @type es: ExpressionSet
    """
    pheno_df = es.get_pheno_data_frame()

    pheno_headers_list = pheno_df.columns.tolist()

    # ng-grid specific
    column_title_to_code_name = {
        val: "_" + hashlib.md5(val).hexdigest()[:8]
        for val in pheno_headers_list
    }

    pheno_headers = [
        {
            "field": column_title_to_code_name[val],
            "displayName": val,
            "minWidth": 150
        }
        for val in pheno_headers_list
    ]

    #pheno_table = json.loads(pheno_df.to_json(orient="records"))
    # again ng-grid specific
    pheno_table = []
    for record in pheno_df.to_records():

        pheno_table.append({
            str(column_title_to_code_name[title]): fix_val(value)
            for (title, value)
            in zip(pheno_headers_list, list(record)[1:])
        })

    return {
        "headers": pheno_headers,
        "headers_title_to_code_map": column_title_to_code_name,
        "table": pheno_table,
        "user_class_title": es.pheno_metadata.get("user_class_title")
    }