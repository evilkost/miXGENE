import rpy2.robjects as R
from rpy2.robjects.packages import importr

class GMT(object):
    def __init__(self, description, gene_sets):
        self.description = description
        self.gene_sets = gene_sets

    def to_r_obj(self):
        gene_sets = R.ListVector(dict([
            (k, R.StrVector(v))
            for k, v in self.gene_sets.iteritems()
        ]))
        return gene_sets


def parse_gmt(filepath):
    description = {}
    gene_sets = {}
    with open(filepath) as inp:
        for line in inp:
            split = line.strip().split("\t")
            key = split[0]
            description[key] = split[1]
            gene_sets[key] = split[2:]

    return GMT(description, gene_sets)
