from sklearn.metrics import accuracy_score, \
    average_precision_score, confusion_matrix, roc_auc_score, \
    f1_score, recall_score, matthews_corrcoef, hamming_loss, jaccard_similarity_score

import logging
log = logging.getLogger(__name__)


class MetricInfo(object):
    def __init__(self, name, func, title,
                 produce_single_number=True, require_binary=True,
                 to_dict=None,
                 **kwargs):
        self.name = name
        self.title = title
        self.func = func
        self.require_binary = require_binary
        self.produce_single_number = produce_single_number
        self.options = kwargs
        if to_dict is not None:
            self.to_dict = to_dict
        else:
            self.to_dict = str

    def apply(self, y_true, y_predicted):
        try:
            return self.func(y_true, y_predicted, **self.options)
        except Exception, e:
            log.exception("Failed to compute metric %s on %s, %s",
                          self.name, y_true, y_predicted)
            return None

metrics = [
    MetricInfo("average_precision", average_precision_score,
               title="AP (average precision)", require_binary=True),
    MetricInfo("AUC", roc_auc_score,
               title="AUC (Area Under the Curve)", require_binary=True),
    MetricInfo("MCC", matthews_corrcoef,
               title="MCC (Matthews correlation coefficient)", require_binary=True),

    # For the following metrics in multiclass case we need to select additional parameters
    # to obtain singular value, so we would limit them only to binary case
    MetricInfo("f1", f1_score,
               title="F1 score", require_binary=True),
    MetricInfo("recall_score", recall_score,
               title="Recall", require_binary=True),

    ### Metrics, which supports multiclass classification
    MetricInfo("accuracy", accuracy_score,
               title="Accuracy", require_binary=False),
    MetricInfo("hamming_loss", hamming_loss,
               title="Average Hamming loss", require_binary=False),
    MetricInfo("jaccard_similarity", jaccard_similarity_score,
               title="Jaccard similarity coefficient", require_binary=False),

    ### Metrics with non-single value results
    MetricInfo("confusion_matrix", confusion_matrix,
               title="Confusion matrix", produce_single_number=False),

]
metrics_dict = {
    metric.name: metric
    for metric in metrics
}


def compute_score(y_true, y_predicted, metric):
    return metric.apply(y_true, y_predicted)


def compute_score_by_metric_name(y_true, y_predicted, metric_name):
    return metrics_dict[metric_name].apply(y_true, y_predicted)


def compute_scores(y_true, y_predicted, metrics_subset=None, is_classes_binary=True):
    result = {}
    for metric in metrics:
        if metrics_subset is not None and metric.name in metrics_subset:
            continue

        if not metric.require_binary or is_classes_binary:
            result[metric.name] = metric.apply(y_true, y_predicted)

    return result


