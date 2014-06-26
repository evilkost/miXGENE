# -*- coding: utf-8 -*-
import json
from workflow.blocks.blocks_pallet import GroupType
from workflow.blocks.fields import FieldType, BlockField, InnerOutputField, InputBlockField, ActionRecord, ActionsList
from workflow.blocks.meta_block import UniformMetaBlock


class CellField(object):
    def __init__(self, label, data_type, name=None, *args, **kwargs):
        self.label = label
        self.name = name
        self.data_type = data_type

    def update_name_from_label(self):
        self.name = self.label.replace(" ", "_")

    def to_dict(self, *args, **kwargs):
        return self.__dict__


class CellsPrototype(object):
    def __init__(self):
        self.cells_list = []

    def add_cell(self, cell):
        self.cells_list.append(cell)

    def to_dict(self, *args, **kwargs):
        return {
            "dict": {cell.name: cell.to_dict() for cell in self.cells_list },
            "list": [cell.to_dict() for cell in self.cells_list],
            "__name__": "CellsPrototype",
        }


class CellInfo(object):
    def __init__(self, label):
        self.label = label
        self.inputs_list = []  # [(field name, dyn created input port on block)]

    def to_dict(self, *args, **kwargs):
        return {
            "label": self.label,
            "inputs_list": self.inputs_list
        }

    def __hash__(self):
        return hash(self.label)

    def __eq__(self, other):
        if not isinstance(other, CellInfo):
            return False
        if other.label == self.label:
            return True

        return False


class CellInfoList(object):
    def __init__(self):
        self.cells = []

    def remove_by_label(self, label):
        self.cells.remove(CellInfo(label))

    def to_dict(self, *args, **kwargs):
        return {
            "dict": {cell.label: cell.to_dict() for cell in self.cells},
            "list": [cell.to_dict() for cell in self.cells]
        }


class CustomIterator(UniformMetaBlock):
    block_base_name = "CUSTOM_ITERATOR"
    name = "Custom iterator"
    has_custom_layout = True

    _ci_block_actions = ActionsList([
        ActionRecord("become_ready", ["valid_params"], "ready"),
    ])

    cells_prototype = BlockField(name="cells_prototype", field_type=FieldType.CUSTOM, init_val=None)
    cells = BlockField(name="cells", field_type=FieldType.CUSTOM, init_val=None)
    is_cells_prototype_defined = BlockField(name="is_cells_prototype_defined",
                                            field_type=FieldType.BOOLEAN, init_val=False)

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "custom_iterator/cell_prototype_definition.html",
        "custom_iterator/cell_dyn_inputs.html"
    ])

    def __init__(self, *args, **kwargs):
        super(CustomIterator, self).__init__(*args, **kwargs)
        self.cells_prototype = CellsPrototype()
        self.cells = CellInfoList()

    def add_cell_prototype_field(self, exp, received_block, *args, **kwargs):
        new_field_dict = received_block.get("cells_prototype", {}).get("new_cell_field")
        if new_field_dict:
            cf = CellField(**new_field_dict)
            cf.update_name_from_label()
            self.cells_prototype.add_cell(cf)
            exp.store_block(self)

    def finish_cells_prototype_definition(self, exp, *args, **kwargs):
        self.is_cells_prototype_defined = True

        for field_prototype in self.cells_prototype.cells_list:
            new_inner_output = InnerOutputField(
                name=field_prototype.name,
                provided_data_type=field_prototype.data_type
            )
            self.register_inner_output_variables([new_inner_output])

        exp.store_block(self)

    def add_cell(self, exp, received_block, *args, **kwargs):
        new_cell_dict = received_block.get("cells", {}).get("new")
        if new_cell_dict:
            cell = CellInfo(new_cell_dict["label"])
            for field_prototype in self.cells_prototype.cells_list:
                new_name = "%s_%s" % (field_prototype.name, len(self.cells.cells))
                cell.inputs_list.append((field_prototype.name, new_name))
                # TODO: add input port to block
                new_port = InputBlockField(
                    name=new_name,
                    required_data_type=field_prototype.data_type,
                    required=True
                )
                self.add_input_port(new_port)

            self.cells.cells.append(cell)
            exp.store_block(self)

    def remove_cell(self, exp, cell_json, *args, **kwargs):
        try:
            cell = json.loads(cell_json)
            self.cells.remove_by_label(cell["label"])

            exp.store_block(self)
        except:
            pass

    def become_ready(self, *args, **kwargs):
        pass

    def on_params_is_valid(self, exp, *args, **kwargs):
        super(CustomIterator, self).on_params_is_valid(exp, *args, **kwargs)
        self.do_action("become_ready", exp, *args, **kwargs)

    def get_fold_labels(self):
        return [cell.label for cell in self.cells.cells]

    def execute(self, exp, *args, **kwargs):
        self.inner_output_manager.reset()
        seq = []
        for cell_def in self.cells.cells:
            cell = {}
            for name, input_var_name in cell_def.inputs_list:
                # TODO: hmm maybe we should create deepcopy?
                cell[name] = self.get_input_var(input_var_name)
            seq.append(cell)
        exp.store_block(self)

        self.do_action("on_folds_generation_success", exp, seq)
