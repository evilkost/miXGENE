from webapp.tasks import wrapper_task
from workflow.blocks.blocks_pallet import GroupType
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputBlockField, InputType, ParamField, \
    ActionRecord, ActionsList
from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

from converters.gene_set_tools import map_gene_sets_to_probes


class MergeGeneSetWithPlatformAnnotation(GenericBlock):
    block_base_name = "MERGE_GS_GPL_ANN"
    name = "Merge gene set with platform"
    block_group = GroupType.PROCESSING

    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])
    _block_actions.extend(execute_block_actions_list)

    _input_gs = InputBlockField(name="gs", order_num=10,
                                required_data_type="GeneSets", required=True)
    _input_ann = InputBlockField(name="ann", order_num=20,
                                 required_data_type="PlatformAnnotation", required=True)

    _gs = OutputBlockField(name="gs", field_type=FieldType.HIDDEN, init_val=None,
                           provided_data_type="GeneSets")

    def __init__(self, *args, **kwargs):
        super(MergeGeneSetWithPlatformAnnotation, self).__init__(*args, **kwargs)
        self.celery_task = None

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        gs, ann = self.get_input_var("gs"), self.get_input_var("ann")
        # import ipdb; ipdb.set_trace()
        self.celery_task = wrapper_task.s(
            map_gene_sets_to_probes,
            exp, self,
            base_dir=exp.get_data_folder(),
            base_filename="%s_merged" % self.uuid,
            ann_gene_sets=ann.gene_sets,
            src_gene_sets=gs

        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, gs):
        self.set_out_var("gs", gs)
        exp.store_block(self)
