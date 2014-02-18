from environment.structures import TableResult
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField

from wrappers.svm import lin_svm_task


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
        lin_svm_options = self.collect_svm_options()
        self.celery_task = lin_svm_task.s(
            exp, self,
            self.get_input_var("train_es"), self.get_input_var("test_es"),  # accessing bound vars
            lin_svm_options,
            exp.get_data_folder(), "%s" % self.uuid,
            target_class_column=None,
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, result, *args, **kwargs):
        # We store obtained result as an output variable
        self.set_out_var("result", result)
        exp.store_block(self)
