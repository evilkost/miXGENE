from environment.structures import TableResult
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputBlockField, InputType, ParamField, \
    ActionRecord, ActionsList
from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

from wrappers.svm import linear_svm


class SvmClassifier(GenericBlock):
    block_base_name = "LIN_SVM"
    is_block_supports_auto_execution = True

    # Block behavior
    _block_actions = ActionsList([])
    _block_actions.extend(save_params_actions_list)
    _block_actions.extend(execute_block_actions_list)

    # User defined parameters
    C = ParamField(name="C", title="Penalty",
                   input_type=InputType.TEXT, field_type=FieldType.FLOAT, init_val=1.0)
    tol = ParamField(name="tol", title="Tolerance for stopping criteria",
                     input_type=InputType.TEXT, field_type=FieldType.FLOAT, init_val=0.0001)

    # Input ports definition
    _train_es = InputBlockField(name="train_es", required_data_type="ExpressionSet",
                                required=True)
    _test_es = InputBlockField(name="test_es", required_data_type="ExpressionSet",
                               required=True)

    # Provided outputs
    _result = OutputBlockField(name="result", field_type=FieldType.CUSTOM,
                               provided_data_type="ClassifierResult", init_val=None)

    # Block sub elements, presentation role
    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "svm_result.html"
    ])

    def __init__(self, *args, **kwargs):
        super(SvmClassifier, self).__init__("Linear Svm Classifier", *args, **kwargs)
        self.celery_task = None

    def collect_svm_options(self):
        options = {}
        svm_options = [
            "C", "tol",  # TODO: add more parameters
        ]
        for p_name in svm_options:
            val = getattr(self, p_name, None)
            if val:
                options[p_name] = val
        return options

    def execute(self, exp,  *args, **kwargs):
        self.set_out_var("result", None)
        lin_svm_options = self.collect_svm_options()
        train_es = self.get_input_var("train_es")
        self.celery_task = wrapper_task.s(
            linear_svm,
            exp, self,
            train_es=train_es,
            test_es=self.get_input_var("test_es"),
            lin_svm_options=lin_svm_options,
            base_folder=exp.get_data_folder(),
            base_filename="%s_svm" % self.uuid,
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