__author__ = 'kost'

from webapp.models import Experiment
from workflow.blocks import get_block_class_by_name

def add_block_to_exp_from_dict(exp, block_dict):
    block_cls = get_block_class_by_name(block_dict["block_name"])
    block = block_cls(exp_id=exp.pk, scope_name=block_dict["scope_name"])

    blocks_uuids = exp.get_all_block_uuids()
    block.base_name = "%s:%s" % (block.block_base_name, len(blocks_uuids) + 1)

    exp.store_block(block, new_block=True)
    return block
