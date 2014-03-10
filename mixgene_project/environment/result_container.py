# -*- coding: utf-8 -*-

import copy
# from webapp.models import Experiment
from collections import defaultdict
import cPickle as pickle

from itertools import product

import numpy as np
import pandas as pd


def get_metric_vectorized(metric):
    def func(x):
        return float(x.scores[metric])

    return np.vectorize(func, otypes=[np.float])


class ResultContainer(object):
    def __init__(self):
        self.ar = np.empty(shape=(), dtype=object)
        self.axis_list = []
        self.labels_dict = {}  # axis -> [label for 0, label for 1] and so on
        self.inverse_labels_dict = defaultdict(dict) # axis -> { label -> idx of element in axis }

    def get_axis_dim(self, axis):
        return self.axis_list.index(axis)

    def store(self, file_obj):
        out = {
            "ar": self.ar.tolist(),
            "axis_list": self.axis_list,
            "labels_dict": self.labels_dict
        }
        pickle.dump(out, file_obj, protocol=pickle.HIGHEST_PROTOCOL)

    def empty_clone(self):
        new_rc = ResultContainer()
        new_rc.ar = np.empty(shape=self.ar.shape, dtype=object)
        new_rc.axis_list = self.axis_list
        new_rc.labels_dict = copy.deepcopy(self.labels_dict)
        new_rc.update_label_index()
        return new_rc

    def clone(self):
        new_rc = self.empty_clone()
        new_rc.ar = copy.deepcopy(self.ar)
        return new_rc

    @staticmethod
    def load(file_obj):
        data = pickle.load(file_obj)

        rc = ResultContainer()
        rc.axis_list = data["axis_list"]
        rc.labels_dict = data["labels_dict"]
        rc.ar = np.array(data["ar"])
        rc.update_label_index()

        return rc

    def update_label_index(self):
        self.inverse_labels_dict = defaultdict(dict)
        for axis, labels_list in self.labels_dict.iteritems():
            for idx, label in enumerate(labels_list):
                self.inverse_labels_dict[axis][label] = idx

    def get_flat_list_by_metric(self, metric):
        """
            Flatten container and access specified metric.
            We expecting only ClassifierResult object, other instances would be ignored
        """
        return [
            cr.scores[metric]
            for cr in self.ar.flatten()
            if metric in cr.scores
        ]

    def dim_num(self, axis_name):
        return self.axis_list.index(axis_name)

    def build_axis_mask(self, spec_dict):
        mask_list = []
        for axis in self.axis_list:
            if axis in spec_dict:
                key = spec_dict[axis]
                if type(key) == str or type(key) == unicode:
                    idx = self.inverse_labels_dict[axis][key]
                elif hasattr(key, "__iter__"):
                    idx = [self.inverse_labels_dict[axis][key_i] for key_i in key]
                else:
                    raise Exception("Gor wrong defenition for axis %s value" % axis)

                mask_list.append(idx)
            else:
                mask_list.append(slice(0, len(self.labels_dict[axis])))
        return tuple(mask_list)

    def filter_by_spec(self, spec_dict):
        """
            Return new ResultCollections
        """

        new_rc = self.empty_clone()
        new_rc.ar = self.ar[self.build_axis_mask(spec_dict)]
        return new_rc

    def get_pandas_slice(self, axis_for_header, axis_list_for_index, metric, method=None):
        """
            Only one axis to be used as headers
            And more than one could be used for rows index
        """
        if method is None:
            method = "avg"

        header_dim = self.get_axis_dim(axis_for_header)
        rows_dim_list = map(self.get_axis_dim, axis_list_for_index)

        shapes = self.ar.shape
        all_dims = range(len(shapes))
        avg_by = tuple(set(all_dims).difference(rows_dim_list + [header_dim]))

        without_avg_by = [e for e in all_dims if e not in avg_by]

        #ref_axis = dict(enumerate(without_avg_by))
        # new_axis_list = [
        #     self.axis_list[idx]
        #     for idx in without_avg_by
        # ]

        float_ar = get_metric_vectorized(metric)(self.ar)

        if method == "avg":
            avg_ar = np.average(float_ar, axis=avg_by)
        else:
            raise NotImplementedError("Not implemented method %s" % method)

        index_labels = [self.labels_dict[axis] for axis in axis_list_for_index]
        df_index = pd.MultiIndex.from_product(index_labels, names=axis_list_for_index)
        df = pd.DataFrame(index=df_index, columns=[self.labels_dict[axis_for_header]])

        for row_def in product(*index_labels):
            spec_def = {axis: val for val, axis in zip(row_def, axis_list_for_index)}
            for column in self.labels_dict[axis_for_header]:
                spec_def[axis_for_header] = column
                key = tuple(np.array(self.build_axis_mask(spec_def))[[without_avg_by]])
                df.ix[row_def, column] = avg_ar[key]

        return df

    @staticmethod
    def from_lower_dim_list(list_of_rcs, axis_name, labels_list):
        rc = list_of_rcs[0]
        new_shape = rc.ar.shape + (len(labels_list), )
        new_ar = np.empty(shape=new_shape, dtype=object)
        for idx, rc in enumerate(list_of_rcs):
            mask = tuple(slice(0, shp) for shp in rc.ar.shape) + (idx,)
            new_ar[mask] = rc.ar

        new_rc = ResultContainer()
        new_rc.ar = new_ar
        new_rc.axis_list = rc.axis_list + [axis_name]
        new_rc.labels_dict = copy.deepcopy(rc.labels_dict)
        new_rc.labels_dict[axis_name] = labels_list
        new_rc.update_label_index()

        return new_rc
