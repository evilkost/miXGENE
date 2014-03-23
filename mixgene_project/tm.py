import copy
# from webapp.models import Experiment
from collections import defaultdict
import cPickle as pickle

from itertools import product

import numpy as np
import pandas as pd

from environment.result_container import ResultsContainer


def main(base_dir, base_filename):
    rc1 = ResultsContainer("", "")
    rc1.ar = np.empty(shape=(2,), dtype=object)
    rc1.axis_list = ["classifiers"]
    rc1.labels_dict["classifiers"] = ["svm", "dt"]

    from environment.structures import ClassifierResult
    c1 = ClassifierResult("", "")
    c1.scores["accuracy"] = 0.8
    c2 = ClassifierResult("", "")
    c2.scores["accuracy"] = 0.95

    rc1.ar[0] = c1
    rc1.ar[1] = c2

    rc2 = copy.deepcopy(rc1)
    rc3 = copy.deepcopy(rc1)
    rc4 = copy.deepcopy(rc1)

    d2_rc = ResultsContainer("", "")
    d2_rc.add_dim_layer([rc1, rc2, rc3, rc4], "cv_folds", ["f1", "f2", "f3", "f4"])

    d2rc1 = copy.deepcopy(d2_rc)
    d2rc2 = copy.deepcopy(d2_rc)
    d2rc3 = copy.deepcopy(d2_rc)


    d3_rc = ResultsContainer(base_dir, base_filename)

    d3_rc.add_dim_layer([d2rc1, d2rc2, d2rc3], "fenotype_features",
                                                ["age", "sex", "tissue"])


    return d3_rc

if __name__ == "__main__":
    r = main(".", "tmp.gz")
    r.store()

    r2 = ResultsContainer(".", "tmp.gz")
    r2.load()
    print r2.to_dict()
    print r2.ar

    #print r2.({"cv_folds": ("f2",)})