import traceback, sys

from sklearn import svm
from sklearn import preprocessing

from celery import task

from environment.structures import ExpressionSet, ClassifierResult
from wrappers.scoring import compute_scores


def linear_svm(exp, block,
        train_es, test_es,
        lin_svm_options,
        base_folder, base_filename,
    ):
    """
        @type train_es: ExpressionSet
        @type test_es: ExpressionSet
    """
    is_class_binary = False
    # if target_class_column is None:
    target_class_column = train_es.pheno_metadata["user_class_title"]

    # Unpack data
    x_train = train_es.get_assay_data_frame().as_matrix().transpose()
    y_train = train_es.get_pheno_data_frame()[target_class_column].as_matrix()

    x_test = test_es.get_assay_data_frame().as_matrix().transpose()
    y_test = test_es.get_pheno_data_frame()[target_class_column].as_matrix()

    if len(set(y_test)) == 2 and len(set(y_train)) == 2:
        is_class_binary = True

    # Unfortunately svm can't operate with string labels as a target classes
    #   so we need to preprocess labels
    le = preprocessing.LabelEncoder()
    le.fit(y_train)

    y_train_fixed = le.transform(y_train)
    y_test_fixed = le.transform(y_test)

    # Classifier initialization
    classifier = svm.LinearSVC(**lin_svm_options)
    # Model learning
    classifier.fit(x_train, y_train_fixed)

    # Applying on test partition
    y_test_predicted = classifier.predict(x_test)

    # Here we build result object
    cr = ClassifierResult(base_folder, base_filename)
    cr.labels_encode_vector = le.classes_  # Store target class labels
    cr.classifier = "linear_svm"
    cr.scores = compute_scores(y_test_fixed, y_test_predicted,
                               is_classes_binary=is_class_binary)  # Hmm what about parametric scores?
    cr.store_model(classifier)
    return [cr], {}
