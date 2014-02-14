from celery import task
from environment.structures import GeneSets, GS
from environment.units import GeneUnits

from mixgene.util import transpose_dict_list


def map_gene_sets_to_probes(base_dir, base_filename, ann_gene_sets, src_gene_sets):
    """
    TODO: working units check

    @param filepath: Filepath to store result obj

    @type ann_gs: GeneSets
    @type gs: GeneSets

    @rtype: GeneSets
    """
    entrez_ids_to_probes_map = transpose_dict_list(ann_gene_sets.get_gs().genes)

    gene_sets_probes = GeneSets(base_dir, base_filename)

    gene_sets_probes.metadata["org"] = src_gene_sets.metadata["org"]
    gene_sets_probes.metadata["gene_units"] = GeneUnits.PROBE_ID
    gene_sets_probes.metadata["set_units"] = src_gene_sets.metadata["set_units"]
    gs = GS()
    src_gs = src_gene_sets.get_gs()
    for set_name, gene_ids in src_gs.genes.iteritems():
        tmp_set = set()
        for entrez_id in gene_ids:
            tmp_set.update(entrez_ids_to_probes_map.get(entrez_id ,[]))
        if tmp_set:
            gs.genes[set_name] = list(tmp_set)
            gs.description[set_name] = src_gs.description[set_name]

    gene_sets_probes.store_gs(gs)

    return gene_sets_probes


def filter_gs_by_genes(src_gs, allowed_genes):
    """
    @type src: environment.structures.GS

    @type allowed_genes: list of strings
    @param allowed_genes: gene units in allowed_genes and src should be the same

    @rtype: environment.structures.GS
    """

    allowed = set(allowed_genes)
    gs = GS()
    for k, gene_set in src_gs.genes.iteritems():
        to_preserve = set(gene_set).intersection(allowed)
        if to_preserve:
            gs.genes[k] = list(to_preserve)
            gs.description[k] = src_gs.description
    return gs


@task(name="converters.gene_set_tools.merge_gs_with_platform_annotation")
def merge_gs_with_platform_annotation(
        exp, block, gene_sets, annotation, base_dir, base_filename,
        success_action="success", error_action="error",
    ):
    """
        @type gene_sets: GeneSets
        @type annotation: PlatformAnnotation
    """
    try:
        gs_merged = map_gene_sets_to_probes(base_dir, base_filename, annotation.gene_sets, gene_sets)

        block.do_action(success_action, exp, gs_merged)
    except Exception, e:
        block.errors.append(e)
        block.do_action(error_action, exp)


