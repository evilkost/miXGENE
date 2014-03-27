# -*- coding: utf-8 -*-

import logging
import traceback, sys

from sklearn.svm import LinearSVC, SVC
from sklearn.naive_bayes import GaussianNB
from sklearn import tree
from sklearn import neighbors
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing

from celery import task

from environment.structures import ExpressionSet, ClassifierResult
from wrappers.scoring import compute_scores

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


classifiers_map = {
    # name -> ( fabric,  apply wrapper, if None use common_apply)
    "linear_svm": (LinearSVC, None),
    "svm": (SVC, None),
    "gaussian_nb": (GaussianNB, None),
    "DT": (tree.DecisionTreeClassifier, None),
    "random_forest": (RandomForestClassifier, None),
    "knn": (neighbors.KNeighborsClassifier, None),
}


def apply_classifier(
    exp, block,
    train_es, test_es,
    classifier_name, classifier_options=None, fit_options=None,
    base_folder="/tmp", base_filename="cl"
):
    """
        @type train_es: ExpressionSet
        @type test_es: ExpressionSet
    """
    if not classifier_options:
        classifier_options = {}
    if not fit_options:
        fit_options = {}

    target_class_column = train_es.pheno_metadata["user_class_title"]


    # Unpack data
    x_train = train_es.get_assay_data_frame().as_matrix().transpose()
    y_train = train_es.get_pheno_data_frame()[target_class_column].as_matrix()

    x_test = test_es.get_assay_data_frame().as_matrix().transpose()
    y_test = test_es.get_pheno_data_frame()[target_class_column].as_matrix()

    # Unfortunately svm can't operate with string labels as a target classes
    #   so we need to preprocess labels
    le = preprocessing.LabelEncoder()
    le.fit(y_train)

    y_train_fixed = le.transform(y_train)
    y_test_fixed = le.transform(y_test)

    # Classifier initialization
    fabric, apply_func = classifiers_map[classifier_name]
    log.debug("Classifier options: %s", classifier_options)
    if apply_func is None:
        cl = fabric(**classifier_options)
        cl.fit(x_train, y_train_fixed, **fit_options)
    else:
        raise NotImplementedError()


    # Applying on test partition
    y_test_predicted = cl.predict(x_test)

    # Here we build result object
    cr = ClassifierResult(base_folder, base_filename)

    cr.labels_encode_vector = le.classes_  # Store target class labels

    cr.y_true = y_test_fixed
    cr.y_predicted = y_test_predicted

    cr.classifier = classifier_name
    cr.store_model(cl)
    return [cr], {}
