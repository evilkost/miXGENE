__author__ = 'kost'

from webapp.models import Experiment
from workflow.blocks import get_block_class_by_name


def add_block_to_exp_from_request(request):
    exp = Experiment.get_exp_from_request(request)
    block_cls = get_block_class_by_name(request.POST['block'])
    block = block_cls(exp_id=exp.e_id, scope=request.POST['scope'])

    blocks_uuids = exp.get_all_block_uuids()
    block.base_name = "%s:%s" % (block.block_base_name, len(blocks_uuids) + 1)


    exp.store_block(block, new_block=True)

    return block