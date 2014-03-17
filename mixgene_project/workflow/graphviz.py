# -*- coding: utf-8 -*-
"""
    Brute-force approach to generate .dot graph definition from block structure.
"""
import logging
from collections import defaultdict

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class Input(object):
    def __init__(self, src, name, dtype, from_cluster=False):
        self.src = src
        self.name = name
        self.dtype = dtype
        self.from_cluster = from_cluster

    @property
    def label(self):
        # return "%s [%s]" % (self.name, self.dtype)
        return "%s" % (self.name, )


class Node(object):
    def __init__(self, name, title=None):
        self.name = name
        self.title = title or name
        self.inputs = []
        self.row_template = " \"%s\" -> \"%s\" [ label=\"%s\" ];\n"
        self.row_template_from_cluster = \
            " \"_dummy_%s\" -> \"%s\" [ label=\"%s\" ltail=\"cluster_%s\"];\n"

    def __str__(self):
        return "Node: %s_%s" % (self.name, self.label)

    def add_input(self, inp):
        self.inputs.append(inp)

    def to_dot(self):
        header = "{node  [shape=box, label=\"%s\" ] \"%s\" };\n" % (self.title, self.name)
        connections = ""
        for input in self.inputs:
            if not input.from_cluster:
                connections += self.row_template % (input.src, self.name, input.label)
            else:
                connections += self.row_template_from_cluster %\
                          (input.src, self.name, input.label, input.src)
        return header, connections

class SubGraph(Node):
    def __init__(self, name, label, is_root=False):
        super(SubGraph, self).__init__(name)
        self.label = label
        self.is_root = is_root
        self.nodes = []

    def __str__(self):
        return "SG: %s_%s" % (self.name, self.label)

    def append(self, node):
        self.nodes.append(node)

    def to_dot(self):
        connections = ""
        nodes_header = []
        nodes_connection = []
        for node in self.nodes:
            h, c = node.to_dot()
            nodes_header.append(h)
            nodes_connection.append(c)

        if self.is_root:
            header = """digraph %s {
node [ shape=record];
graph [fontsize=10 fontname="Verdana" compound=true ratio=compress size="14,20!"];
""" % self.name
            header += "".join(nodes_header)
            header += "".join(nodes_connection)
            header += "}\n"
        else:
            header = "subgraph cluster_%s {\n label=\"%s\" ;\n" % \
                     (self.name, self.label)
            header += "color=blue;\n"
            header += "{ node  [ style=invis ]  \"_dummy_%s\" };\n" % self.name
            header += "".join(nodes_header)
            header += "}\n"

            connections += "".join(nodes_connection)
            for inp in self.inputs:
                connections += " \"%s\" -> \"%s\" [ label=\"%s\",  lhead=\"cluster_%s\"];\n" %\
                    (inp.src, "_dummy_%s" % self.name, inp.label, self.name)

        return header, connections


def root_from_exp(exp):
    blocks_dict = dict(exp.get_blocks(exp.get_all_block_uuids()))
    root = SubGraph("root", "", True)

    node_by_uuid = {}

    iterated_input_node_dict = {}
    collector_node_dict = {}

    scope_creating_nodes = {"root": root}
    blocks_by_scope = defaultdict(list)
    for uuid, block in blocks_dict.iteritems():
        blocks_by_scope[block.scope_name].append(block)
        if block.create_new_scope:
            sg_node = SubGraph(uuid, block.name, False)
            sg_input_node = Node("%s_inner" % uuid, "Single iteration variables")
            iterated_input_node_dict[uuid] = sg_input_node
            sg_collector_node = Node("%s_collector" % uuid, "Collect iteration results")
            collector_node_dict[uuid] = sg_collector_node
            sg_node.append(sg_input_node)
            sg_node.append(sg_collector_node)

            node_by_uuid[uuid] = sg_node
            scope_creating_nodes[block.sub_scope_name] = sg_node

    for uuid, block in blocks_dict.iteritems():
        parent_sg = scope_creating_nodes[block.scope_name]
        if block.create_new_scope:
            node = scope_creating_nodes[block.sub_scope_name]
        else:
            node = Node(uuid, block.name)
            node_by_uuid[uuid] = node

        parent_sg.append(node)

    for uuid, block in blocks_dict.iteritems():
        node = node_by_uuid[uuid]
        for input_name, bound_var in block.bound_inputs.iteritems():
            # log.debug("Adding edge to %s-%s from %s", block.base_name, input_name, bound_var)
            parent_uuid = bound_var.block_uuid
            parent_block = blocks_dict[parent_uuid]
            if parent_block.scope_name != block.scope_name and \
                    block.create_new_scope and not parent_block.create_new_scope and \
                    parent_block.out_manager.contains(bound_var.var_name):

                new_input = Input(parent_uuid, input_name, bound_var.data_type,
                                  from_cluster=parent_block.create_new_scope)
                node.add_input(new_input)

            elif parent_block.scope_name != block.scope_name and \
                    parent_block.create_new_scope:
                # inner block access iterated inputs
                sg_input_node = iterated_input_node_dict[parent_uuid]
                new_input = Input(sg_input_node.name, input_name, bound_var.data_type)
                node.add_input(new_input)
            else:
                new_input = Input(parent_uuid, input_name, bound_var.data_type,
                                  from_cluster=parent_block.create_new_scope)
                node.add_input(new_input)

        if block.create_new_scope:
            for input_name, bound_var in block.collector_spec.bound.iteritems():
                sg_collector_node = collector_node_dict[uuid]
                parent_uuid = bound_var.block_uuid
                parent_block = blocks_dict[parent_uuid]

                new_input = Input(parent_uuid, input_name, bound_var.data_type,
                                  from_cluster=parent_block.create_new_scope)
                sg_collector_node.add_input(new_input)

    return root
