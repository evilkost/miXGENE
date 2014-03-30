# -*- coding: utf-8 -*-

import copy
# from webapp.models import Experiment
from collections import defaultdict
import cPickle as pickle
import functools
import gzip
import logging

from itertools import product
#from functools import reduce

import numpy as np
import pandas as pd

from environment.structures import GenericStoreStructure, ClassifierResult
from wrappers.scoring import compute_score_by_metric_name

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def get_metric_vectorized(metric):
    def func(x):
        try:
            return float(x.scores[metric])
        except Exception, e:
            log.exception("Failed to get value from `%s` metric: `%s` with error: %s",
                          x.scores, metric, e)
            # import  ipdb; ipdb.set_trace()
            return np.nan

    return np.vectorize(func, otypes=[np.float])


class ResultsContainer(GenericStoreStructure):
    def __init__(self, *args, **kwargs):
        super(ResultsContainer, self).__init__(*args, **kwargs)
        self.filepath = self.form_filepath("_res_cont")

        self.ar = np.empty(shape=(), dtype=object)
        self.axis_list = []
        self.labels_dict = {}  # axis -> [label for 0, label for 1] and so on
        self.inverse_labels_dict = defaultdict(dict) # axis -> { label -> idx of element in axis }

    def init_ar(self):
        shape = tuple(map(len, [
            self.labels_dict[axis]
            for axis in self.axis_list
        ]))
        # noinspection PyNoneFunctionAssignment
        self.ar = np.empty(shape=shape, dtype=object)

    def get_axis_dim(self, axis):
        """
            @return: axis idx in array shape
            @rtype: int
        """
        return self.axis_list.index(axis)

    def store(self):
        with gzip.open(self.filepath, "w") as file_obj:
            out = {
                "ar": self.ar.tolist(),
                "axis_list": self.axis_list,
                "labels_dict": self.labels_dict
            }
            pickle.dump(out, file_obj, protocol=pickle.HIGHEST_PROTOCOL)

    def empty_clone(self, base_filename):
        new_rc = ResultsContainer(self.base_dir, base_filename)

        new_rc.axis_list = self.axis_list
        new_rc.labels_dict = copy.deepcopy(self.labels_dict)
        new_rc.init_ar()
        new_rc.update_label_index()
        return new_rc

    def clone(self, base_filename):
        new_rc = self.empty_clone(base_filename)
        self.load()
        new_rc.ar = copy.deepcopy(self.ar)
        new_rc.store()
        return new_rc

    def load(self):
        try:
            with gzip.open(self.filepath) as file_obj:
                pickled = file_obj.read()
                data = pickle.loads(pickled)
                self.axis_list = data["axis_list"]
                self.labels_dict = data["labels_dict"]

                self.ar = np.array(data["ar"])
                self.update_label_index()

        except Exception, e:
            log.exception(e)
            raise Exception("Failed to load result container from path: `%s`:", self.filepath)

    def update_label_index(self):
        self.inverse_labels_dict = defaultdict(dict)
        for axis, labels_list in self.labels_dict.iteritems():
            # log.debug("Labels list: %s", labels_list)
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

    def build_axis_mask(self, spec_dict, axis_list=None):
        mask_list = []
        if not axis_list:
            axis_list = self.axis_list
        for axis in axis_list:
            if axis in spec_dict:
                key = spec_dict[axis]
                if type(key) == str or type(key) == unicode:
                    idx = self.inverse_labels_dict[axis][key]
                elif hasattr(key, "__iter__"):
                    idx = [self.inverse_labels_dict[axis][key_i] for key_i in key]
                else:
                    raise Exception("Gor wrong definition for axis %s value" % axis)

                mask_list.append(idx)
            else:
                mask_list.append(slice(0, len(self.labels_dict[axis])))
        return tuple(mask_list)

    def filter_by_spec(self, spec_dict):
        new_rc = self.empty_clone()
        new_rc.ar = self.ar[self.build_axis_mask(spec_dict)]
        return new_rc

    def aggregate_prediction_vectors(self, axis_list_to_preserve):
        """
            Produce new np.array be merging `y_true`, `y_predicted` fields of ClassifierResult objects
                in axis that not present in axis_list_to_preserve. New array is reshaped to complain
                with axis order in axis_list_to_preserve.

            @return: Array of len(axis_axis_list_to_preserve) dimensions
                each element [ClassifierResult] would have joined y_true and y_predicted vectors
            @rtype: np.array
        """

        if len(axis_list_to_preserve) == len(self.axis_list):
            return np.transpose(self.ar, [
                self.axis_list.index(axis)
                for axis in axis_list_to_preserve
            ])

        new_shape = tuple([
            len(self.labels_dict[axis_name])
            for axis_name in axis_list_to_preserve
        ])
        result = np.empty(shape=new_shape, dtype=object)
        index_labels = [self.labels_dict[axis] for axis in axis_list_to_preserve]

        for row_def in product(*index_labels):
            # log.debug("processing row: %s", row_def)
            spec_def = {axis: val for val, axis in zip(row_def, axis_list_to_preserve)}
            key = np.array(self.build_axis_mask(spec_def))
            key_for_result = tuple([
                self.inverse_labels_dict[axis_name][label]
                for label, axis_name
                in zip(row_def, axis_list_to_preserve)
            ])
            sliced = self.ar[tuple(key)]
            if hasattr(sliced, 'flatten'):
                flatten = sliced.flatten()
                new_cr = ClassifierResult("", "")
                new_cr.classifier = "aggregated_result"
                for cr in flatten:
                    # new_cr.labels_encode_vector.extend(cr.labels_encode_vector)
                    new_cr.y_true.extend(cr.y_true)
                    new_cr.y_predicted.extend(cr.y_predicted)

                # import  ipdb; ipdb.set_trace()
                result[key_for_result] = new_cr

        return result

    def compute_score(self, axis_list_to_preserve, metric_name, propogate_error=False):
        """
            @return: Array with computed score or NA if we failed to compute score
            @rtype: np.array
        """
        agg_ar = self.aggregate_prediction_vectors(axis_list_to_preserve)

        scoring_function = functools.partial(
            compute_score_by_metric_name, metric_name=metric_name)

        def func(cr):
            """
                @type cr: ClassifierResult
                @rtype: float
            """
            try:
                return scoring_function(cr.y_true, cr.y_predicted)
            except Exception, e:
                if propogate_error:
                    raise e
                return np.nan

        vectorized_func = np.vectorize(func, otypes=[np.float])

        score_ar = vectorized_func(agg_ar)
        return score_ar

    def get_pandas_slice_for_boxplot(
            self,
            compare_axis_by_boxplot,
            agg_axis_for_scoring,
            metric_name
    ):
        axis_list_to_preserve = set(self.axis_list).difference(agg_axis_for_scoring)
        log.debug("Axis to preserve: %s", axis_list_to_preserve)
        score_ar = self.compute_score(axis_list_to_preserve, metric_name=metric_name)

        index_labels = [self.labels_dict[axis] for axis in compare_axis_by_boxplot]
        df_index = pd.MultiIndex.from_product(index_labels, names=compare_axis_by_boxplot)
        cols_num = reduce(lambda x, y: x * y, [
            len(self.labels_dict[axis_name]) for axis_name
            in set(axis_list_to_preserve).difference(compare_axis_by_boxplot)
        ])
        df = pd.DataFrame(index=df_index, columns=map(str, range(cols_num)))

        for row_def in product(*index_labels):
            spec_def = {axis: val for val, axis in zip(row_def, compare_axis_by_boxplot)}
            key = np.array(self.build_axis_mask(spec_def, axis_list=axis_list_to_preserve))
            sliced = score_ar[tuple(key)]
            df.ix[row_def, :] = np.array(sliced.flatten())

        return df

    def get_pandas_slice(self, axis_for_header, axis_list_for_index, metric_name):
        """
            Only one axis to be used as headers
            And more than one could be used for rows index
        """
        index_labels = [self.labels_dict[axis] for axis in axis_list_for_index]
        df_index = pd.MultiIndex.from_product(index_labels, names=axis_list_for_index)
        df = pd.DataFrame(index=df_index, columns=[self.labels_dict[axis_for_header]])

        axis_list_to_preserve = axis_list_for_index + [axis_for_header]
        score_ar = self.compute_score(axis_list_to_preserve, metric_name=metric_name)

        for row_def in product(*index_labels):
            spec_def = {axis: val for val, axis in zip(row_def, axis_list_for_index)}
            for column in self.labels_dict[axis_for_header]:
                spec_def[axis_for_header] = column
                key = tuple(np.array(self.build_axis_mask(spec_def, axis_list_to_preserve)))

                val = score_ar[key]
                df.ix[row_def, column] = val

        return df

    def add_dim_layer(self, list_of_rcs, axis_name, labels_list):
        rc = list_of_rcs[0]

        self.axis_list = rc.axis_list + [axis_name]
        self.labels_dict = copy.deepcopy(rc.labels_dict)
        self.labels_dict[axis_name] = labels_list
        self.update_label_index()
        self.init_ar()

        for idx, rc in enumerate(list_of_rcs):
            mask = tuple(slice(0, shp) for shp in rc.ar.shape) + (idx,)
            self.ar[mask] = rc.ar

    def to_dict(self, *args, **kwargs):
        self.load()
        return {
            "labels_dict": self.labels_dict,
            "shape": self.ar.shape,
            "axis_list": self.axis_list
        }

    def export_to_json_dict(self, *args, **kwargs):
        self.load()

        meta = self.to_dict()
        log.debug("Meta: %s", meta)
        records = []
        index_labels = [self.labels_dict[axis] for axis in self.axis_list]
        for row_def in product(*index_labels):
            spec_def = {axis: val for val, axis in zip(row_def, self.axis_list)}
            key = tuple(np.array(self.build_axis_mask(spec_def)))
            val = self.ar[key]
            log.debug("%s -> %s", (key, val))

            cell = {
                "key": key,
                "key_names": spec_def,
                "y_true": list(val.y_true),
                "y_predicted": list(val.y_predicted),

            }
            records.append(cell)
        return {
            "meta": meta,
            "records": records,
        }
