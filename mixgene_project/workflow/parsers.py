import rpy2.robjects as R
from rpy2.robjects.packages import importr

class GMT(object):
    def __init__(self, description=None, gene_sets=None):
        self.description = description
        self.gene_sets = gene_sets

        self.org = ""
        self.units = ""

    def to_r_obj(self):
        gene_sets = R.ListVector(dict([
            (k, R.StrVector(v))
            for k, v in self.gene_sets.iteritems()
        ]))

        mgs = R.r['new']('mixGeneSets')
        mgs.do_slot_assign("gene.sets", gene_sets)
        mgs.do_slot_assign("org", R.StrVector([self.org]))
        mgs.do_slot_assign("units", R.StrVector([self.units]))

        return mgs

    def write_file(self, filepath):
        with open(filepath, "w") as output:
            for key, description in self.description.iteritems():
                output.write("%s\t%s\t%s\n" % (
                    (key, description, "\t".join(self.gene_sets[key]))
                ))


def parse_gmt(filepath):
    description = {}
    gene_sets = {}
    with open(filepath) as inp:
        for line in inp:
            split = line.strip().split("\t")
            if len(split) < 3:
                continue
            key = split[0]
            description[key] = split[1]
            gene_sets[key] = split[2:]

    return GMT(description, gene_sets)
