from sklearn.metrics import accuracy_score, average_precision_score, confusion_matrix, auc

import logging
log = logging.getLogger(__name__)


class MetricInfo(object):
    def __init__(self, name, func, produce_single_number=True, require_binary=True, **kwargs):
        self.name = name
        self.func = func
        self.require_binary = require_binary
        self.produce_single_number = produce_single_number
        self.options = kwargs

    def apply(self, y_true, y_predicted):
        try:
            return self.func(y_true, y_predicted, **self.options)
        except Exception, e:
            log.exception("Failed to compute metric %s on %s, %s",
                          self.name, y_true, y_predicted)
            return None

metrics = [
    MetricInfo("accuracy", accuracy_score, require_binary=False),
    MetricInfo("AUC", auc, require_binary=False),
    MetricInfo("average_precision", average_precision_score, require_binary=True)
    # ("confusion_matrix", confusion_matrix),
]


def compute_scores(y_true, y_predicted, is_classes_binary=True):
    result = {}
    for metric in metrics:
        if not metric.require_binary or is_classes_binary:
            result[metric.name] = metric.apply(y_true, y_predicted)

    return result


