from collections import defaultdict
import copy
import cPickle as pickle
import logging

from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.notification import AllUpdated

# TODO: invent magic to correct logging when called outside of celery task

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

LOCK_TIME = 60*10 # 10 minutes


class ScopeVar(object):
    def __init__(self, block_uuid, var_name,
                 data_type=None, block_alias=None,
                 scope_name=None):
        self.block_uuid = block_uuid
        self.var_name = var_name
        self.data_type = data_type
        self.block_alias = block_alias
        self.scope_name = scope_name

    @property
    def title(self):
        return "%s -> %s" % (self.block_alias or self.block_uuid, self.var_name)

    @property
    def pk(self):
        return str(self)

    @staticmethod
    def from_key(key):
        split = key.split(":")
        block_uuid, rest = split[0], split[1:]
        var_name = "".join(rest)
        return ScopeVar(block_uuid, var_name)

    def to_dict(self):
        result = copy.deepcopy(self.__dict__)
        result["pk"] = self.pk
        result["title"] = self.title
        return result

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if self.block_uuid == other.block_uuid and self.var_name == other.var_name:
            return True
        return False

    def __str__(self):
        return ":".join(map(str, [self.block_uuid, self.var_name]))


class DAG(object):
    def __init__(self):
        self.graph = defaultdict(set)  # parent -> [children list]
        self.parents = defaultdict(set)  # child -> [parents list]
        self.roots = set()

        self.topological_order = None
        self.marks = {}

    def reverse_add(self, node, parents=None):
        if node not in self.graph:
            self.graph[node] = set()
        if not parents:
            self.parents[node] = set()
        else:
            self.parents[node].update(parents)
            for parent in parents:
                self.graph[parent].add(node)
                if parent not in self.parents.keys():
                    self.parents[parent] = set()

    def get_children(self, node):
        return self.graph[node]

    def get_parents(self, node):
        return self.parents[node]

    @staticmethod
    def from_deps(dependencies):
        dag = DAG()
        for node, parents in dependencies.iteritems():
            dag.reverse_add(node, parents)

        dag.update_roots()

        dag.sort_graph()

        return dag

    def update_roots(self):
        for node, parents in self.parents.iteritems():
            if not parents:
                self.roots.add(node)

    def get_unmarked(self):
        for node, mark in self.marks.iteritems():
            if mark == "unmarked":
                return node
        return None

    def sort_graph(self):
        # http://en.wikipedia.org/wiki/Topological_sorting
        # TODO( low priority): change to iterative version
        self.topological_order = []
        self.marks = {node: "unmarked" for node in self.graph.keys()}
        while self.get_unmarked():
            n = self.get_unmarked()
            self.visit(n)

    def visit(self, n):
        if self.marks[n] == "temp":
            raise RuntimeError("Contains loop")
        if self.marks[n] == "unmarked":
            self.marks[n] = "temp"
            for child in self.graph[n]:
                self.visit(child)
            self.marks[n] = "perm"
            self.topological_order.insert(0, n)


class ScopeRunner(object):
    def __init__(self, exp, scope_name):
        """
            @type exp: webapp.models.Experiment
            @type scope_name: str
        """
        self.exp = exp
        self.scope_name = scope_name
        self.dag = None

    def is_block_inputs_are_satisfied(self, block_uuid, blocks_dict):
        result = True
        for p_uuid in self.dag.get_parents(block_uuid):
            # TODO: Fix this add hoc code to ignore meta block status
            if blocks_dict[p_uuid].create_new_scope and\
                    blocks_dict[p_uuid].sub_scope_name in self.scope_name:
                continue

            parent_status = blocks_dict[p_uuid].get_exec_status()
            if parent_status != "done":
                result = False
                break

        return result

    def execute(self, is_init_action=False):
        self.build_dag(self.exp.build_block_dependencies_by_scope(self.scope_name))

        blocks_to_execute = []
        working_blocks = []

        blocks_dict = dict(self.exp.get_blocks(self.exp.get_all_block_uuids()))
        for block_uuid in self.dag.topological_order:
            block = blocks_dict[block_uuid]
            if is_init_action and block.is_block_supports_auto_execution and block.get_exec_status() == "done":
                block.do_action("reset_execution", self.exp)

            if block.get_exec_status() == "ready" and \
                    self.is_block_inputs_are_satisfied(block_uuid, blocks_dict):
                blocks_to_execute.append(block)
            if block.get_exec_status() == "working" and \
                    self.is_block_inputs_are_satisfied(block_uuid, blocks_dict):
                working_blocks.append(block)

        if not blocks_to_execute and not working_blocks:
            log.debug("Nothing to execute in scope `%s` for exp `%s`",
                      self.scope_name, self.exp.pk)
            if self.scope_name != "root":
                block = self.exp.get_meta_block_by_sub_scope(self.scope_name)
                block.do_action("on_sub_scope_done", self.exp)
            else:
                AllUpdated(self.exp.pk, comment=u"Workflow execution completed", silent=False).send()
        elif blocks_to_execute:
            # for block in blocks_to_execute:
            #     print "Block %s will be executed" % block.name
            #     block.do_action("execute", self.exp
            blocks_to_execute[0].do_action("execute", self.exp)

    def build_dag(self, block_dependencies):
        """
            @type block_dependencies: dict
            @param block_dependencies: {block -> parents}
        """
        self.dag = DAG()
        for node, parents in block_dependencies.iteritems():
            self.dag.reverse_add(node, parents)

        self.dag.update_roots()
        self.dag.sort_graph()
        # self.dag.p1()


class Scope(object):
    # TODO: change webapp.models.py into package and move Scope there
    def __init__(self, exp, scope_name):
        """
            @type exp: webapp.models.Experiment
            @type scope_name: str
        """
        self.exp = exp
        self.name = scope_name

        self.scope_vars = set()

    @property
    def vars_by_data_type(self):
        result = defaultdict(set)
        for scope_var in self.scope_vars:
            result[scope_var.data_type].add(scope_var)

        return result

    def store(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key = ExpKeys.get_scope_key(self.exp.pk, scope_name=self.name)
        r.set(key, pickle.dumps(self.scope_vars))
        log.debug("Scope `%s` was saved to storage", self.name)

        #import ipdb; ipdb.set_trace()
        #a = 2

    def get_parent_scope_list(self, redis_instance=None):
        if self.name == "root":
            return []

        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        parent_scope_names = []
        wn_scope_name = self.name
        while True:
            scope_creating_block = self.exp.get_meta_block_by_sub_scope(wn_scope_name, r)
            wn_scope_name = scope_creating_block.scope_name
            parent_scope_names.append(wn_scope_name)
            if wn_scope_name == "root":
                break
        return parent_scope_names

    def load(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key = ExpKeys.get_scope_key(self.exp.pk, scope_name=self.name)
        raw = r.get(key)
        if raw is not None:
            self.scope_vars = pickle.loads(raw)
            # TODO: set scope name during scope_var creation
            for scope_var in self.scope_vars:
                scope_var.scope_name = self.name

    def to_dict(self):
        return {
            "name": self.name,
            "vars": [var.to_dict() for var in self.scope_vars],
            "by_data_type": {
                data_type: [var.to_dict() for var in vars]
                for data_type, vars in self.vars_by_data_type.iteritems()
            },
            "by_var_key": {
                var.pk: var.to_dict()
                for var in self.scope_vars
            }
        }

    def update_scope_vars_by_block_aliases(self, aliases_map):
        """
            @param aliases_map: {block_uuid -> alias}
            @type aliases_map: dict
        """
        for scope_var in self.scope_vars:
            if scope_var.block_uuid in aliases_map:
                scope_var.block_alias = aliases_map[scope_var.block_uuid]

    def register_variable(self, scope_var):
        """
            @type scope_var: ScopeVar
        """
        if scope_var in self.scope_vars:
            pass
            #raise KeyError("Scope var %s already registered" % scope_var)
        else:
            log.debug("Variable: `%s` was registered in scope: `%s`", scope_var, self.name)
            self.scope_vars.add(scope_var)
            self.vars_by_data_type[scope_var.data_type].add(scope_var)
