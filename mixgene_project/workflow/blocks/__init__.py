from fetch_gse import FetchGSE
from fetch_bi_gs import GetBroadInstituteGeneSet
from crossvalidation import CrossValidation
from merge_gene_set_annotation import MergeGeneSetWithPlatformAnnotation

from globaltest import GlobalTest
from user_upload import UserUpload, UserUploadComplex, UploadInteraction, UploadGeneSets
from expression_sets_merge import MergeExpressionSets
from workflow.blocks.aggregation import SubAggregation, SvdAggregation
from workflow.blocks.aggregation import GeneSetAgg
from workflow.blocks.classifiers import GaussianNb, DecisionTree, RandomForest, \
    KnnClassifier, LinearSVM, KernelSvm
from workflow.blocks.custom_iterator import CustomIterator
from workflow.blocks.filter_by_bi import FilterByInteraction
from workflow.blocks.mass_upload import MassUpload

from workflow.blocks.multi_features import MultiFeature
from workflow.blocks.box_plot import BoxPlot
from workflow.blocks.pca_visualize import PcaVisualize
from workflow.blocks.rc_table import RenderTable
from workflow.blocks.feature_selection import SvmrfeRanking, \
    SvmrfeRestrictedRanking, TTestRanking, RandomRanking, FeatureSelectionByCut
from workflow.blocks.table_result_view import TableResultView


from workflow.blocks.blocks_pallet import block_classes_by_name, blocks_by_group


def get_block_class_by_name(name):
    if name in block_classes_by_name.keys():
        return block_classes_by_name[name]
    else:
        raise KeyError("No such plugin: %s" % name)

