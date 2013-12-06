import rpy2.robjects as R
from rpy2.robjects.packages import importr
from workflow.parsers import parse_gmt
from pandas import DataFrame

from mixgene.settings import R_LIB_CUSTOM_PATH

R.r['options'](warn=-1)

#TODO: maybe this approach is better ?
"""

class FileVar(object):
    def __init__(self, name, filename, file_type, obj_type=None, *args, **kwargs):
        #
        #    @file_type: Format of physical file
        #    @obj_type:  Type of represented object
        self.name = name
        self.filename = filename
        self.file_type = file_type

"""

rnew = R.r["new"]
rtable = R.r["read.table"]


class MetaMixin(object):
    def copy_meta_from(self, other):
        attrs_list = [
            "org",
            "units",
            "filename",
            #"filepath",
            "delimiter",
            "has_row_names",
            "has_col_names",
        ]

        for attr in attrs_list:
            if hasattr(self, attr) and hasattr(other, attr):
                setattr(self, attr, getattr(other, attr))


class SetNameMixin(object):
    def set_filename(self, exp, filename):
        self.filename = filename
        self.filepath = exp.get_data_file_path(filename)

class MixData(MetaMixin, SetNameMixin):
    def __init__(self):
        self.r_class = "mixData"

        self.org = []
        self.units = []

        # data representation
        self.filepath = None
        self.filename = None
        self.delimiter = " "
        self.has_row_names = True
        self.has_col_names = True

    def to_r_obj(self):
        importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
        r_obj = rnew(self.r_class)

        r_obj.do_slot_assign("org", R.StrVector(self.org))
        r_obj.do_slot_assign("units", R.StrVector(self.units))

        data_frame = rtable(self.filepath, sep=self.delimiter, header=self.has_col_names)
        r_obj.do_slot_assign("data", R.r["data.matrix"](data_frame))
        return r_obj

    def to_data_frame(self):
        return DataFrame.from_csv(self.filepath, sep=self.delimiter)


class MixPheno(MetaMixin, SetNameMixin):
    def __init__(self):
        self.r_class = "mixPheno"

        self.org = []
        self.units = []

        # phenotype representation
        self.filepath = None
        self.filename = None
        self.delimiter = " "
        self.has_row_names = True
        self.has_col_names = True

    def to_r_obj(self):
        importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
        r_obj = rnew(self.r_class)

        r_obj.do_slot_assign("org", R.StrVector(self.org))
        r_obj.do_slot_assign("units", R.StrVector(self.units))

        fact_vec = rtable(self.filepath, sep=self.delimiter, header=self.has_col_names)[0]
        r_obj.do_slot_assign("phenotype", fact_vec)

        return r_obj

    def to_data_frame(self):
        return DataFrame.from_csv(self.filepath, sep=self.delimiter)


class GeneSetsOld(MetaMixin, SetNameMixin):
    def __init__(self):
        """
            Stores in .gmt file format
        """
        self.r_class = "mixGeneSets"

        self.gene_units = None
        self.set_units = None

        self.filename = None
        self.filepath = None

    def get_gmt(self):
        return parse_gmt(self.filepath)

    def to_r_obj(self):
        gmt = parse_gmt(self.filepath)
        gmt.units = self.gene_units
        return gmt.to_r_obj()
