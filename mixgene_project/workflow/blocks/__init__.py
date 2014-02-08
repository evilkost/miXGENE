from collections import defaultdict

from generic import GroupType
from fetch_gse import FetchGSE
from fetch_bi_gs import GetBroadInstituteGeneSet
from crossvalidation import CrossValidation
from merge_gene_set_annotation import MergeGeneSetWithPlatformAnnotation
from pca_visualise import PCA_visualize
from svm_classifier import SvmClassifier
from globaltest import GlobalTest


from collections import defaultdict
from uuid import uuid1


class ActionRecord(object):
    def __init__(self, name, src_states, dst_state, show_to_user=False, **kwargs):
        self.name = name
        self.src_states = src_states
        self.dst_state = dst_state
        self.show_to_user = show_to_user
        self.kwargs = kwargs


class ActionsList(object):
    def __init__(self, actions_list):
        self.actions_list = actions_list

    def contribute_to_class(self, cls, name):
        for action_record in self.actions_list:
            getattr(cls, "_trans").register(action_record)

class TransSystem(object):
    """
        Assumptions:
            - one action can have multiple source states, but only one result state
    """
    states_to_actions = defaultdict(set)
    action_to_state = dict()
    is_action_visible = dict()

    @classmethod
    def register(cls, ar):
        """
            @type ar: ActionRecord
        """
        for state in ar.src_states:
            cls.states_to_actions[state].add(ar.name)
        cls.action_to_state[ar.name] = ar.dst_state
        cls.is_action_visible[ar.name] = ar.show_to_user

    @classmethod
    def user_visible(cls, state):
        return [
            action for action in cls.states_to_actions.get(state, [])
            if cls.is_action_visible[action]
        ]

    @classmethod
    def is_action_available(cls, state, action):
        if action in cls.states_to_actions[state]:
            return True
        else:
            return False

    @classmethod
    def next_state(cls, state, action):
        if action in cls.states_to_actions[state]:
            return cls.action_to_state[action]
        else:
            return None


class BlockField(object):
    def __init__(self, name, field_type, init_val, *args, **kwargs):
        self.name = name
        self.field_type = field_type
        self._value = init_val
        self.is_immutable = kwargs.get("is_immutable", False)
        self.is_exported = kwargs.get("is_exported", True)

    def contribute_to_class(self, cls, name):
        setattr(cls, name, self._value)
        getattr(cls, "_bs").register(self)


class BlockSerializer(object):
    def __init__(self):
        self.fields = dict()

    def register(self, field):
        """
            @type field: BlockField
        """
        self.fields[field.name] = field

    def to_dict(self, block):
        result = {}
        for f_name, f in self.fields.iteritems():
            raw_val = getattr(block, f_name)
            if f.field_type in ["str", "int", "float"]:
                result[f_name] = str(raw_val)
            if f.field_type == "simple_dict":
                result[f_name] = {(str(k), str(v)) for k, v in raw_val.iteritems()}
            if f.field_type == "simple_list":
                result[f_name] = map(str, raw_val)
        return result




class BlockMeta(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(BlockMeta, cls).__new__
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})

        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)

        return new_class

    def add_to_class(cls, name, value):
        #print cls._bs
        if hasattr(value, "contribute_to_class"):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)


class BaseBlock(object):
    _bs = BlockSerializer()
    _trans = TransSystem()
    __metaclass__ = BlockMeta




class GenericBlock(BaseBlock):
    _common_actions = ActionsList([
        ActionRecord("execute", ["ready"], "working"),
        ActionRecord("success", ["working"], "done"),
        ActionRecord("failure", ["working"], "ready"),
    ])

    # block fields
    uuid = BlockField("uuid", "str", None)
    name = BlockField("name", "str", None)
    exp_id = BlockField("exp_id", "str", None)

    scope = BlockField("scope", "str", "root", is_immutable=True)
    state = BlockField("state", "str", "created")
    base_name = BlockField("state", "str", "")

    def __init__(self, name, exp_id, scope):
        """
            Building block for workflow
        """
        self.uuid = uuid1().hex
        self.name = name
        self.exp_id = exp_id

        # pairs of (var name, data type, default name in context)
        self.require_inputs = []
        self.provide_outputs = []

        self.errors = []
        self.warnings = []
        self.base_name = ""

        self.ports = {}  # {group_name -> [BlockPort1, BlockPort2]}
        self.params = {}

    def to_dict(self):
        return self._bs.to_dict(self)

    def do_action(self, action_name, *args, **kwargs):
        next_state = self._trans.next_state(self.state, action_name)
        if next_state is not None:
            self.state = next_state
            getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Action %s isn't available" % action_name)

    def save_form(self, *args, **kwargs):
        pass


save_form_actions_list = ActionsList([
    ActionRecord("save_form", ["created", "form_modified"], "validating_form", True),
    ActionRecord("on_form_is_valid", ["validating_form"], "valid_form"),
    ActionRecord("on_form_not_valid", ["validating_form"], "form_modified"),
])


class Y(GenericBlock):
    _save_form_actions = save_form_actions_list

if __name__ == "__main__":
    y = Y(1, 2, 3)
    print(y.to_dict())
    y.do_action("save_form")
    print(y.to_dict())


#""" OLDER """
block_classes_by_name = {}
blocks_by_group = defaultdict(list)


def register_block(code_name, human_title, group, cls):
    block_classes_by_name[code_name] = cls
    blocks_by_group[group].append({
        "name": code_name,
        "title": human_title,
    })

def get_block_class_by_name(name):
    if name in block_classes_by_name.keys():
        return block_classes_by_name[name]
    else:
        raise KeyError("No such plugin: %s" % name)


register_block("fetch_ncbi_gse", "Fetch from NCBI GEO", GroupType.INPUT_DATA, FetchGSE)
register_block("get_bi_gene_set", "Get MSigDB gene set", GroupType.INPUT_DATA, GetBroadInstituteGeneSet)

register_block("cross_validation", "Cross validation K-fold", GroupType.META_PLUGIN, CrossValidation)

register_block("Pca_visualize", "2D PCA Plot", GroupType.VISUALIZE, PCA_visualize)

register_block("svm_classifier", "Linear SVM Classifier", GroupType.CLASSIFIER, SvmClassifier)

register_block("merge_gs_platform_annotation", "Merge Gene Set with platform",
               GroupType.PROCESSING, MergeGeneSetWithPlatformAnnotation)
register_block("globaltest", "Global test", GroupType.PROCESSING, GlobalTest)
#""" OLDER  END"""