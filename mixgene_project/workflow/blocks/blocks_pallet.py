from collections import defaultdict
import logging


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

block_classes_by_name = {}
blocks_by_group = defaultdict(list)


def add_block_to_toolbox(code_name, human_title, group, cls):
    """
        Registers block to the toolbox pallet
    """
    block_classes_by_name[code_name] = cls
    blocks_by_group[group].append({
        "name": code_name,
        "title": human_title,
    })


class GroupType(object):
    INPUT_DATA = "Input data"
    META_PLUGIN = "Meta block"
    VISUALIZE = "Visualize"
    CLASSIFIER = "Classifier"
    PROCESSING = "Data processing"
