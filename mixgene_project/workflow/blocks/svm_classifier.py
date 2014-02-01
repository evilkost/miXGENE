from fysom import Fysom

from workflow.ports import BlockPort
from workflow.wrappers import svm_test

from generic import GenericBlock


class SvmClassifier(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variables', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'finished', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'run_svm', 'src': 'variable_bound', 'dst': 'running_svm'},
            #{'name': 'run_svm', 'src': 'running_svm', 'dst': 'running_svm'},
            {'name': 'run_svm', 'src': 'svm_done', 'dst': 'running_svm'},

            {'name': 'on_svm_done', 'src': 'running_svm', 'dst': 'svm_done'},
            {'name': 'on_svm_error', 'src': 'running_svm', 'dst': 'variable_bound'},

            ]
    })
    block_base_name = "LIN_SVM"
    all_actions = [
        ("bind_variables", "Select variable", True),
        ("run_svm", "Run SVM", True),

        ("on_svm_done", "", False),
        ("on_svm_error", "", False),


        ("success", "", False),
        ("error", "", False)
    ]
    provided_objects = {
        "mixMlResult": "mixML",
        }
    elements = [
        "svm_result.html"
    ]

    def __init__(self, *args, **kwargs):
        super(SvmClassifier, self).__init__("Linear Svm Classifier", *args, **kwargs)

        self.ports = {
            "input": {
                "train": BlockPort(name="train", title="Choose expression set",
                                   data_type="ExpressionSet", scopes=[self.scope]),
                "test": BlockPort(name="test", title="Choose expression set",
                                  data_type="ExpressionSet", scopes=[self.scope]),
            }
        }

        self.mixMlResult = None
        self.celery_task = None


    def serialize(self, exp, to="dict"):
        hash = super(SvmClassifier, self).serialize(exp, to)
        hash["accuracy"] = ""
        if self.state == "svm_done":
            hash["accuracy"] = self.mixMlResult.acc

        return hash

    def run_svm(self, exp, *args, **kwargs):
        train = self.get_var_by_bound_key_str(exp, self.ports["input"]["train"].bound_key)
        test = self.get_var_by_bound_key_str(exp, self.ports["input"]["test"].bound_key)
        self.celery_task = svm_test.s(exp, self, train, test)
        exp.store_block(self)
        self.celery_task.apply_async()

    def on_svm_error(self, exp, *args, **kwargs):
        exp.store_block(self)

    def on_svm_done(self, exp, *args, **kwargs):
        self.clean_errors()
        exp.store_block(self)