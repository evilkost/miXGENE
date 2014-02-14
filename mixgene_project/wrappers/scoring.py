from sklearn.metrics import accuracy_score, average_precision_score, confusion_matrix

metrics = [
    ("accuracy", accuracy_score),
    ("average_precision", average_precision_score),
    # ("confusion_matrix", confusion_matrix),
]


def compute_scores(y_true, y_predicted):
    result = {}
    for name, func in metrics:
        result[name] = func(y_true, y_predicted)
    return result


