from collections import defaultdict
import copy
import cPickle as pickle
from pprint import pprint

from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance


class ScopeVar(object):
    def __init__(self, block_uuid, var_name, data_type=None, block_alias=None):
        self.block_uuid = block_uuid
        self.var_name = var_name
        self.data_type = data_type
        self.block_alias = block_alias

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
        self.graph = defaultdict(list) # parent -> children list
        self.parents = defaultdict(list) # child -> parent
        self.roots = set()

    def reverse_add(self, node, parents=None):
        self.graph[node].append([])
        if not parents:
            self.parents[node] = []
        else:
            self.parents[node].extend(parents)
            for parent in parents:
                self.graph[parent].append(node)
                self.parents[parent].append([])

    def update_roots(self):
        for node, parents in self.parents.iteritems():
            if not parents:
                self.roots.add(node)

    # def check_loops(self):
    #     for root in self.roots:
    #         visited = set([])
    #         closed = set([])
    #         open_list = [root, ]
    #         # TODO: USE REAL ALGO!!!!!
    #
    #         while open_list:
    #             wn = open_list.pop(0)
    #             if wn in closed:
    #                 raise RuntimeError("Loop detected")
    #             else:
    #                 visited.add(wn)
    #                 for new_node in self.graph[wn]:
    #                     if new_node not in open_list:
    #                         open_list.append(new_node)
    #
    #             if all([n in visited for n in self.parents[wn]]):
    #                 closed.add(wn)

    def get_unmarked(self):
        for node, mark in self.marks.iteritems():
            if mark == "unmarked":
                return node
        return None

    def check_loops(self):
        # http://en.wikipedia.org/wiki/Topological_sorting
        self.L = []
        self.marks = {node: "unmarked" for node in  self.graph.keys()}
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
            self.L.insert(0, n)

    def p1(self):
        pprint(("ROOTS:", self.roots))
        pprint(dict(self.graph))
        pprint(dict(self.parents))
        pprint(("TOPO SORT:", self.L))



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
        self.dag = None

    def run(self):
        self.build_dag()

    def build_dag(self):
        self.dag = DAG()
        for uuid, block in self.exp.get_blocks(self.exp.get_all_block_uuids()):
            if block.scope_name != self.name:
                continue
            else:
                self.dag.reverse_add(block.uuid, block.get_input_blocks())

        self.dag.update_roots()
        self.dag.check_loops()
        self.dag.p1()

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
        print "SAVED"
        #import ipdb; ipdb.set_trace()
        #a = 2

    def load(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key = ExpKeys.get_scope_key(self.exp.pk, scope_name=self.name)
        raw = r.get(key)
        if raw is not None:
            self.scope_vars = pickle.loads(raw)

    def to_dict(self):
        return {
            "name": self.name,
            "vars": [var.to_dict() for var in self.scope_vars],
            "by_data_type": {
                data_type: [var.to_dict() for var in vars]
                for data_type, vars in self.vars_by_data_type.iteritems()
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
            print "REGISTERED"
            self.scope_vars.add(scope_var)
            self.vars_by_data_type[scope_var.data_type].add(scope_var)



