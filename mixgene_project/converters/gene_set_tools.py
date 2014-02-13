from celery import task
from environment.structures import GeneSets, GS
from environment.units import GeneUnits

from mixgene.util import transpose_dict_list


def map_gene_sets_to_probes(base_dir, base_filename, ann_gs, src_gs):
    """
    TODO: working units check

    @param filepath: Filepath to store result obj

    @type ann_gs: GeneSets
    @type gs: GeneSets

    @rtype: GeneSets
    """
    entrez_ids_to_probes_map = transpose_dict_list(ann_gs.genes)

    gene_sets_probes = GeneSets(base_dir, base_filename)

    gene_sets_probes.org = src_gs.org
    gene_sets_probes.units = GeneUnits.PROBE_ID
    gs = GS()
    for set_name, gene_ids in src_gs.genes.iteritems():
        tmp_set = set()
        for entrez_id in gene_ids:
            tmp_set.update(entrez_ids_to_probes_map.get(entrez_id ,[]))
        if tmp_set:
            gs.genes[set_name] = list(tmp_set)
            gs.description[set_name] = gs.description[set_name]

    gene_sets_probes.store_gs(gs)

    return gene_sets_probes


def filter_gs_by_genes(src, allowed_genes):
    """
    @type src: environment.structures.GeneSets

    @type allowed_genes: list of strings
    @param allowed_genes: gene units in allowed_genes and src should be the same

    @rtype: environment.structures.GeneSets
    """

    allowed = set(allowed_genes)
    gene_sets = GeneSets()
    gene_sets.org = src.org
    gene_sets.units = src.units
    gs = GS()

    for k, gene_set in src.genes.iteritems():
        to_preserve = set(gene_set).intersection(allowed)
        if to_preserve:
            gene_sets.genes[k] = list(to_preserve)
            gene_sets.description[k] = src.description
    return gene_sets

@task(name="converters.gene_set_tools.merge_gs_with_platform_annotation")
def merge_gs_with_platform_annotation(
        exp, block, store_field,
        gene_set,
        annotation,
        base_dir, base_filename,
        success_action="success", error_action="error",
    ):
    """
        @type gene_set: GeneSets
        @type annotation: PlatformAnnotation
    """
    try:
        gs = gene_set.get_gs()
        ann_gs = annotation.get_gmt().load() # TODO: rename method in PlatformAnnotation
        gs_merged = map_gene_sets_to_probes(base_dir, base_filename, ann_gs, gs)

        block.do_action(success_action, exp, gs_merged)
    except Exception, e:
        block.errors.append(e)
        block.do_action(error_action, exp)


