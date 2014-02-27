# -*- coding: utf-8 -*-

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField

from wrappers.aggregation import aggregation_task


class SvdAggregation(GenericBlock):
    block_base_name = "SVD_AGG"
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])
    _block_actions.extend(execute_block_actions_list)

    _mRNA_es = InputBlockField(name="mRNA_es", required_data_type="ExpressionSet", required=True)
    _miRNA_es = InputBlockField(name="miRNA_es", required_data_type="ExpressionSet", required=True)
    _interaction = InputBlockField(name="interaction", required_data_type="BinaryInteraction", required=True)

    c = ParamField(name="c", title="Constant c",
                   input_type=InputType.TEXT, field_type=FieldType.FLOAT, init_val=1.0)

    agg_es = OutputBlockField(name="agg_es", provided_data_type="ExpressionSet")

    def __init__(self, *args, **kwargs):
        super(SvdAggregation, self).__init__("Svd aggregation", *args, **kwargs)
        self.celery_task = None

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        mRNA_es = self.get_input_var("mRNA_es")
        miRNA_es = self.get_input_var("miRNA_es")
        interaction_matrix = self.get_input_var("interaction")

        self.celery_task = aggregation_task.s(
            exp, self, "SVD", self.c,
            mRNA_es, miRNA_es, interaction_matrix,
            "%s_svd_agg" % self.uuid,
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, agg_es):
        self.set_out_var("agg_es", agg_es)
        exp.store_block(self)