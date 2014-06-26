from environment.structures import TableResult
from webapp.models import Experiment
from webapp.tasks import wrapper_task
from workflow.blocks.blocks_pallet import GroupType
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputBlockField, InputType, ParamField, \
    ActionRecord, ActionsList
from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

from wrappers.gt import global_test_task


class GlobalTest(GenericBlock):
    block_base_name = "GLOBAL_TEST"
    name = "Goeman global test"
    block_group = GroupType.PROCESSING
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])
    _block_actions.extend(execute_block_actions_list)

    _input_es = InputBlockField(name="es", order_num=10,
                                required_data_type="ExpressionSet", required=True)
    _input_gs = InputBlockField(name="gs", order_num=20,
                                required_data_type="GeneSets", required=True)

    _result = OutputBlockField(name="result", field_type=FieldType.STR,
                               provided_data_type="TableResult", init_val=None)

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "gt_result.html"
    ])

    def __init__(self, *args, **kwargs):
        super(GlobalTest, self).__init__(*args, **kwargs)
        self.celery_task = None

        exp = Experiment.get_exp_by_id(self.exp_id)
        self.result = TableResult(
            base_dir=exp.get_data_folder(),
            base_filename="%s_gt_result" % self.uuid,
        )
        self.result.headers = ['p-value', 'Statistic', 'Expected', 'Std.dev', '#Cov']

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        self.celery_task = wrapper_task.s(
            global_test_task,
            exp, self,
            es=self.get_input_var("es"),
            gene_sets=self.get_input_var("gs"),
            table_result=self.result
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, result, *args, **kwargs):
        self.result = result
        self.set_out_var("result", self.result)
        exp.store_block(self)
