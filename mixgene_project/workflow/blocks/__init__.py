from collections import defaultdict

from generic import GroupType
from fetch_gse import FetchGSE
from fetch_bi_gs import GetBroadInstituteGeneSet
from crossvalidation import CrossValidation
from merge_gene_set_annotation import MergeGeneSetWithPlatformAnnotation
# from pca_visualise import PCA_visualize

from globaltest import GlobalTest
from user_upload import UserUpload, UserUploadComplex, UploadInteraction, UploadGeneSets
from expression_sets_merge import MergeExpressionSets
from workflow.blocks.classifiers import GaussianNb, DecisionTree, RandomForest,\
    KnnClassifier, LinearSVM, KernelSvm
from workflow.blocks.custom_iterator import CustomIterator
from workflow.blocks.filter_by_bi import FilterByInteraction
from workflow.blocks.mass_upload import MassUpload

from workflow.blocks.multi_features import MultiFeature
from workflow.blocks.sub_agg import SubAggregation
from workflow.blocks.svd_agg import SvdAggregation
from workflow.blocks.box_plot import BoxPlot
from workflow.blocks.rc_table import RenderTable
from workflow.blocks.feature_selection import SvmrfeRanking, \
    SvmrfeRestrictedRanking, TTestRanking, RandomRanking, FeatureSelectionByCut

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
#register_block("user_upload", "Upload dataset", GroupType.INPUT_DATA, UserUpload)

register_block("user_upload_complex", "Upload mRna/miRna/methyl dataset ", GroupType.INPUT_DATA, UserUploadComplex)
register_block("upload_interaction", "Upload gene interaction", GroupType.INPUT_DATA, UploadInteraction)
register_block("upload_gs", "Upload gene sets", GroupType.INPUT_DATA, UploadGeneSets)
register_block("get_bi_gene_set", "Get MSigDB gene set", GroupType.INPUT_DATA, GetBroadInstituteGeneSet)

register_block("cross_validation", "Cross validation K-fold", GroupType.META_PLUGIN, CrossValidation)
register_block("multi_feature", "Multi feature validation", GroupType.META_PLUGIN, MultiFeature)
register_block("custom_iterator", "Custom iterator", GroupType.META_PLUGIN, CustomIterator)
register_block("mass_upload", "Mass upload", GroupType.META_PLUGIN, MassUpload)

# register_block("Pca_visualize", "2D PCA Plot", GroupType.VISUALIZE, PCA_visualize)
register_block("box_plot", "Box plot ML scores", GroupType.VISUALIZE, BoxPlot)
register_block("rc_table", "Results container as table", GroupType.VISUALIZE, RenderTable)

register_block("linear_svm", "Linear SVM Classifier", GroupType.CLASSIFIER, LinearSVM)
register_block("svm", "Kernel SVM Classifier", GroupType.CLASSIFIER, KernelSvm)
register_block("gaussian_nb", "Gaussian Naive Bayes", GroupType.CLASSIFIER, GaussianNb)
register_block("dt_block", "Decision Tree", GroupType.CLASSIFIER, DecisionTree)
register_block("rnd_forest", "Random Forest", GroupType.CLASSIFIER, RandomForest)
register_block("knn", "Knn Classifier", GroupType.CLASSIFIER, KnnClassifier)

register_block("merge_gs_platform_annotation", "Merge Gene Set with platform",
               GroupType.PROCESSING, MergeGeneSetWithPlatformAnnotation)
register_block("globaltest", "Global test", GroupType.PROCESSING, GlobalTest)
register_block("svd_agg", "Svd aggregation", GroupType.PROCESSING, SvdAggregation)
register_block("sub_agg", "Subtractive aggregation", GroupType.PROCESSING, SubAggregation)
register_block("merge_two_es", "Merge expression sets", GroupType.PROCESSING, MergeExpressionSets)
register_block("filter_by_bi", "Filter ES by interaction", GroupType.PROCESSING, FilterByInteraction)

register_block("svmrfe", "SVMRFE Ranking", GroupType.PROCESSING, SvmrfeRanking)
register_block("res_svmrfe", "Restricted SVMRFE", GroupType.PROCESSING, SvmrfeRestrictedRanking)
register_block("ttest_rank", "TTest ranking", GroupType.PROCESSING, TTestRanking)
register_block("randm_rank", "Random ranking", GroupType.PROCESSING, RandomRanking)
register_block("fs_by_cut", "Feature selection, cut by threshold", GroupType.PROCESSING, FeatureSelectionByCut)
