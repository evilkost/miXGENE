from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField

from converters.gene_set_tools import merge_gs_with_platform_annotation


class MergeGeneSetWithPlatformAnnotation(GenericBlock):
    block_base_name = "MERGE_GS_GPL_ANN"

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])
    _block_actions.extend(execute_block_actions_list)

    _input_gs = InputBlockField(name="gs", required_data_type="GeneSets", required=True)
    _input_ann = InputBlockField(name="ann", required_data_type="PlatformAnnotation", required=True)

    _gs = OutputBlockField(name="gs", field_type=FieldType.HIDDEN, init_val=None,
                           provided_data_type="GeneSets")

    def __init__(self, *args, **kwargs):
        super(MergeGeneSetWithPlatformAnnotation, self).__init__(
            "Merge GeneSet with platform annotation", *args, **kwargs)
        self.celery_task = None

    def execute(self, exp, request, *args, **kwargs):
        self.clean_errors()
        gs, ann = self.get_input_var("gs"), self.get_input_var("ann")
        # import ipdb; ipdb.set_trace()
        self.celery_task = merge_gs_with_platform_annotation.s(
            exp, self, gs, ann, exp.get_data_folder(), "%s_merged" % self.uuid
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, gs):
        self.set_out_var("gs", gs)
        exp.store_block(self)
