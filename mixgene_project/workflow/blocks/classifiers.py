# -*- coding: utf-8 -*-
from abc import abstractmethod
import logging

from environment.structures import TableResult
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputBlockField, InputType, ParamField, \
    ActionRecord, ActionsList
from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

from wrappers.svm import linear_svm

from wrappers.sk_classifiers import apply_classifier

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class GenericClassifier(GenericBlock):
    is_block_supports_auto_execution = True
    classifier_name = ""
    # Block behavior
    _block_actions = ActionsList([])
    _block_actions.extend(save_params_actions_list)
    _block_actions.extend(execute_block_actions_list)

    # User defined parameters
    # Input ports definition
    _train_es = InputBlockField(name="train_es", order_num=10,
                                required_data_type="ExpressionSet",
                                required=True)
    _test_es = InputBlockField(name="test_es", order_num=20,
                               required_data_type="ExpressionSet",
                               required=True)

    # Provided outputs
    _result = OutputBlockField(name="result", field_type=FieldType.CUSTOM,
                               provided_data_type="ClassifierResult", init_val=None)

    def __init__(self, *args, **kwargs):
        super(GenericClassifier, self).__init__(*args, **kwargs)

        self.celery_task = None
        self.classifier_options = {}
        self.fit_options = {}

    @abstractmethod
    def collect_options(self):
        """
            Should populate `self.classifier_options` and `self.fit_options`
            from block parameters.
        """
        pass
        # options = {}
        # svm_options = [
        #     "C", "tol",  # TODO: add more parameters
        # ]
        # for p_name in svm_options:
        #     val = getattr(self, p_name, None)
        #     if val:
        #         options[p_name] = val
        # return options

    def execute(self, exp,  *args, **kwargs):
        self.set_out_var("result", None)
        self.collect_options()

        train_es = self.get_input_var("train_es")
        test_es = self.get_input_var("test_es")

        self.celery_task = wrapper_task.s(
            apply_classifier,
            exp=exp, block=self,

            train_es=train_es, test_es=test_es,

            classifier_name=self.classifier_name,
            classifier_options=self.classifier_options,
            fit_options=self.fit_options,

            base_folder=exp.get_data_folder(),
            base_filename="%s_%s" % (self.uuid, self.classifier_name),
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, result, *args, **kwargs):
        # We store obtained result as an output variable
        self.set_out_var("result", result)
        exp.store_block(self)

    def reset_execution(self, exp, *args, **kwargs):
        self.clean_errors()
        self.set_out_var("result", None)
        exp.store_block(self)


class GaussianNb(GenericClassifier):
    block_base_name = "GAUSSIAN_NB"
    classifier_name = "gaussian_nb"

    def __init__(self, *args, **kwargs):
        super(GaussianNb, self).__init__("Gaussian Naive Bayes", *args, **kwargs)

    def collect_options(self):
        pass


class DecisionTree(GenericClassifier):
    block_base_name = "DT"
    classifier_name = "DT"

    criterion = ParamField(
        name="criterion",
        title="The function to measure the quality of a split",
        input_type=InputType.SELECT,
        field_type=FieldType.STR,
        order_num=11,
        options={
            "inline_select_provider": True,
            "select_options": [
                ["gini", "Gini impurity"],
                ["entropy", "Information gain"]
            ]
        }
    )

    max_features_mode = ParamField(
        name="max_features_mode",
        title="The number of features to consider when looking for the best split",
        input_type=InputType.SELECT,
        field_type=FieldType.STR,
        options={
            "inline_select_provider": True,
            "select_options": [
                ["int", "Fixed number"],
                ["float", "Ratio of the features number [0.0 .. 1.0]"],
                ["sqrt", "sqrt(number of features)"],
                ["log2", "log2(number of features)"],
                ]
        },
        order_num=20,
    )

    max_features_value = ParamField(
        name="max_features_value",
        title="Value for the chosen mode",
        input_type=InputType.TEXT,
        field_type=FieldType.STR,
        order_num=30,
    )

    max_depth = ParamField(
        name="max_depth",
        title="The maximum depth of the tree",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=40,
    )

    min_samples_split = ParamField(
        name="min_samples_split",
        title="The minimum number of samples to split an internal node",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=50,
    )

    min_samples_leaf = ParamField(
        name="min_samples_leaf",
        title="The minimum number of samples to be at a leaf node",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=60,
    )

    def __init__(self, *args, **kwargs):
        super(DecisionTree, self).__init__("Decision tree", *args, **kwargs)

    def collect_options(self):
        if hasattr(self, "max_features_mode"):
            mode = str(self.max_features_mode)
            if mode in ["sqrt", "log2"]:
                self.classifier_options["max_features"] = mode
            else:
                raw_value = getattr(self, "max_max_features_value", None)
                if raw_value:
                    if mode == "int":
                        self.classifier_options["max_features"] = int(raw_value)
                    if mode == "float":
                        self.classifier_options["max_features"] = float(raw_value)

        if hasattr(self, "max_depth"):
            self.classifier_options["max_depth"] = int(self.max_depth)

        if hasattr(self, "min_samples_split"):
            self.classifier_options["min_samples_split"] = int(self.min_samples_split)

        if hasattr(self, "min_samples_leaf"):
            self.classifier_options["min_samples_leaf"] = int(self.min_samples_leaf)


class RandomForest(GenericClassifier):
    block_base_name = "RND_FOREST"
    classifier_name = "random_forest"

    n_estimators = ParamField(
        name="n_estimators",
        title="The number of trees in the forest",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        init_val=10,
        order_num=10,
    )

    criterion = ParamField(
        name="criterion",
        title="The function to measure the quality of a split",
        input_type=InputType.SELECT,
        field_type=FieldType.STR,
        order_num=11,
        options={
            "inline_select_provider": True,
            "select_options": [
                ["gini", "Gini impurity"],
                ["entropy", "Information gain"]
            ]
        }
    )

    max_features_mode = ParamField(
        name="max_features_mode",
        title="The number of features to consider when looking for the best split",
        input_type=InputType.SELECT,
        field_type=FieldType.STR,
        options={
            "inline_select_provider": True,
            "select_options": [
                ["int", "Fixed number"],
                ["float", "Ratio of the features number [0.0 .. 1.0]"],
                ["sqrt", "sqrt(number of features)"],
                ["log2", "log2(number of features)"],
            ]
        },
        order_num=20,
    )

    max_features_value = ParamField(
        name="max_features_value",
        title="Value for the chosen mode",
        input_type=InputType.TEXT,
        field_type=FieldType.STR,
        order_num=30,
    )

    max_depth = ParamField(
        name="max_depth",
        title="The maximum depth of the tree.",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=40,
    )

    min_samples_split = ParamField(
        name="min_samples_split",
        title="The minimum number of samples to split an internal node",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=50,
    )

    min_samples_leaf = ParamField(
        name="min_samples_leaf",
        title="The minimum number of samples to be at a leaf node",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=60,
    )

    def __init__(self, *args, **kwargs):
        super(RandomForest, self).__init__("Random forest", *args, **kwargs)

    def collect_options(self):
        if hasattr(self, "n_estimators"):
            self.classifier_options["n_estimators"] = int(self.n_estimators)

        if hasattr(self, "max_features_mode"):
            mode = str(self.max_features_mode)
            if mode in ["sqrt", "log2"]:
                self.classifier_options["max_features"] = mode
            else:
                raw_value = getattr(self, "max_max_features_value", None)
                if raw_value:
                    if mode == "int":
                        self.classifier_options["max_features"] = int(raw_value)
                    if mode == "float":
                        self.classifier_options["max_features"] = float(raw_value)

        if hasattr(self, "max_depth"):
            self.classifier_options["max_depth"] = int(self.max_depth)

        if hasattr(self, "min_samples_split"):
            self.classifier_options["min_samples_split"] = int(self.min_samples_split)

        if hasattr(self, "min_samples_leaf"):
            self.classifier_options["min_samples_leaf"] = int(self.min_samples_leaf)



class KnnClassifier(GenericClassifier):
    block_base_name = "KNN"
    classifier_name = "knn"

    n_neighbors = ParamField(
        name="n_neighbors",
        title="Number of neighbors",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        init_val=1,
        order_num=10,
    )

    algorithm = ParamField(
        name="algorithm",
        title="Algorithm [optional]",
        input_type=InputType.SELECT,
        field_type=FieldType.STR,
        order_num=20,
        options={
            "inline_select_provider": True,
            "select_options": [
                 ["ball_tree", "BallTree"],
                 ["kd_tree", "KDTree"],
                 ["brute", "Brute force search"],
                 ["auto", "Auto guess algorithm"],
            ]
        }
    )

    leaf_size = ParamField(
        name="leaf_size",
        title="Leaf size for BallTree or KDTree [optional]",
        input_type=InputType.TEXT,
        field_type=FieldType.INT,
        order_num=30,
    )

    _metric_options = BlockField(name="metric_options", field_type=FieldType.RAW)
    metric_options = [
        {"pk": "euclidean", "str": "Euclidean Distance"},
        {"pk": "manhattan", "str": "Manhattan Distance"},
        {"pk": "chebyshev", "str": "Chebyshev Distance"},
    ]
    metric = ParamField(
        name="metric",
        title="The distance metric to use for the tree [optional]",
        input_type=InputType.SELECT,
        field_type=FieldType.STR,
        select_provider="metric_options",
        order_num=40,
        options={
            "inline_select_provider": True,
            "select_options": [
                ["euclidean", "Euclidean Distance"],
                ["manhattan", "Manhattan Distance"],
                ["chebyshev", "Chebyshev Distance"],
            ]
        }
    )

    def __init__(self, *args, **kwargs):
        super(KnnClassifier, self).__init__("Knn classifier", *args, **kwargs)

    def collect_options(self):
        if hasattr(self, "n_n_neighbors") and self.n_neighbors:
            self.classifier_options["n_neighbors"] = int(self.n_neighbors)
        if hasattr(self, "algorithm") and self.algorithm:
            self.classifier_options["algorithm"] = self.algorithm
        if hasattr(self, "leaf_size") and self.leaf_size:
            self.classifier_options["leaf_size"] = int(self.leaf_size)
        if hasattr(self, "metric") and self.metric:
            self.classifier_options["metric"] = self.metric

