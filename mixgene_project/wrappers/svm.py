import traceback, sys

from sklearn import svm
from sklearn import preprocessing

from celery import task

from environment.structures import ExpressionSet, ClassifierResult
from wrappers.scoring import compute_scores


def linear_svm(train_es, test_es,
               lin_svm_options,
               base_folder, base_filename,
               target_class_column=None):
    """
        @type train_es: ExpressionSet
        @type test_es: ExpressionSet
    """

    if target_class_column is None:
        target_class_column = "User_class"

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
    classifier = svm.LinearSVC(**lin_svm_options)
    # Model learning
    classifier.fit(x_train, y_train_fixed)

    # Applying on test partition
    y_test_predicted = classifier.predict(x_test)

    # Here we build result object
    cr = ClassifierResult(base_folder, base_filename)
    cr.labels_encode_vector = le.classes_  # Store target class labels
    cr.classifier = "linear_svm"
    cr.scores = compute_scores(y_test_fixed, y_test_predicted)  # Hmm what about parametric scores?
    cr.store_model(classifier)
    return cr


## Here is a Celery task wrapper, it will be simplified in the future
@task(name="wrappers.svm.lin_svm_task")
def lin_svm_task(exp, block,
                 train_es, test_es,
                 lin_svm_options,
                 base_folder, base_filename,
                 target_class_column=None,
                 success_action="success", error_action="error"
    ):
    try:
        classifier_result = linear_svm(train_es, test_es,
                                       lin_svm_options,
                                       base_folder, base_filename,
                                       target_class_column)
        block.do_action(success_action, exp, classifier_result)
    except Exception, e:
        block.do_action(error_action, exp, e)
