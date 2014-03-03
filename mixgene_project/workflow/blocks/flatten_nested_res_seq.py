# -*- coding: utf-8 -*-
from environment.structures import SequenceContainer

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField

from workflow.common_tasks import wrapper_task

class FlattenNestedResultSequences(GenericBlock):
    block_base_name = "FLATTEN_RES_SEQ"
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
        ])
    _block_actions.extend(execute_block_actions_list)

    _res_seq = InputBlockField(name="res_seq", required_data_type="SequenceContainer", required=True)
    flatten_seq = OutputBlockField(name="flatten_seq", field_type=FieldType.HIDDEN, init_val=None,
                           provided_data_type="SequenceContainer")

    def __init__(self, *args, **kwargs):
        super(FlattenNestedResultSequences, self).__init__(
            "Flatten nested SequenceContainer by merging cells with more attributes", *args, **kwargs)
        self.celery_task = None

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        in_res_seq = self.get_input_var("res_seq")

        nested_seq_name = [
            name for name, dtype in
            in_res_seq.fields.iteritems() if dtype == "SequenceContainer"
        ][0]
        new_fields = {}

        cell_0 = in_res_seq.sequence[0]

        map_old_pos_to_new_fields = {}  # (fold idx, sub_field_name) -> new_field_name

        sub_seq = cell_0[nested_seq_name]
        for sub_field_name in sub_seq.fields:
            sub_field_type = sub_seq.fields[sub_field_name]
            for idx, cell in enumerate(in_res_seq.sequence):
                label = cell.get("__label__", str(idx))
                new_field_name = "%s_%s" % (label, sub_field_name)
                new_fields.update({new_field_name: sub_field_type})

                map_old_pos_to_new_fields[idx, sub_field_name] = new_field_name

        new_sequence = [
            {"__label__": sub_cell.get("__label__", idx)}
            for idx, sub_cell in
            enumerate(sub_seq.sequence)
        ]

        for outer_idx, cell in enumerate(in_res_seq.sequence):
            sub_seq = cell[nested_seq_name]
            for idx, sub_cell in enumerate(sub_seq.sequence):
                for sub_field_name in sub_seq.fields.keys():
                    new_field_name = map_old_pos_to_new_fields[outer_idx, sub_field_name]
                    new_sequence[idx][new_field_name] = sub_cell[sub_field_name]

        new_seq = SequenceContainer(new_fields, new_sequence)
        self.do_action("success", exp, new_seq)

    def success(self, exp, new_seq):
        self.set_out_var("flatten_seq", new_seq)
        exp.store_block(self)
