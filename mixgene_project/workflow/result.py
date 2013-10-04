from uuid import uuid1
from rpy2 import robjects as R


class mixPlot(object):
    def __init__(self, exp, rMixPlot, csv_filename):
        self.uuid = str(uuid1())
        self.template = "workflow/result/mixPlot.html"
        self.title = "mixPlot"

        self.main = rMixPlot.do_slot('main')[0]
        self.caption = rMixPlot.do_slot('caption')[0]

        # haven't implimented .do_slot('cl') since points already has this data
        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)

        R.r['write.table'](rMixPlot.do_slot('points'), self.filepath, row_names=True, col_names=True)

        self.has_col_names = True
        self.has_row_names = True
        self.csv_delimiter = " "


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


class mixTable(object):
    def __init__(self, exp, rMixTable, csv_filename):
        self.uuid = str(uuid1())
        self.template = "workflow/result/mixTable.html"
        self.title = "mixTable"

        self.caption = rMixTable.do_slot('caption')[0]
        self.working_units = list(rMixTable.do_slot('working.units'))

        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)
        rMixTable.do_slot('table').to_csvfile(self.filepath, sep=" ")

        self.has_col_names = True
        self.has_row_names = True
        self.csv_delimiter = " "