from collections import defaultdict

from generic import GroupType
from fetch_gse import FetchGSE
from fetch_bi_gs import GetBroadInstituteGeneSet
from crossvalidation import CrossValidation
from merge_gene_set_annotation import MergeGeneSetWithPlatformAnnotation
# from pca_visualise import PCA_visualize
from svm_classifier import SvmClassifier
from globaltest import GlobalTest
from user_upload import UserUpload, UserUploadComplex, UploadInteraction
from expression_sets_merge import MergeExpressionSets


#""" OLDER """
from workflow.blocks.multi_features import MultiFeature
from workflow.blocks.sub_agg import SubAggregation
from workflow.blocks.svd_agg import SvdAggregation

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
register_block("user_upload", "Upload dataset", GroupType.INPUT_DATA, UserUpload)

register_block("user_upload_complex", "Upload mRna/miRna/methyl dataset ", GroupType.INPUT_DATA, UserUploadComplex)
register_block("upload_interaction", "Upload gene interaction", GroupType.INPUT_DATA, UploadInteraction)
register_block("get_bi_gene_set", "Get MSigDB gene set", GroupType.INPUT_DATA, GetBroadInstituteGeneSet)

register_block("cross_validation", "Cross validation K-fold", GroupType.META_PLUGIN, CrossValidation)
register_block("multi_feature", "Multi feature validation", GroupType.META_PLUGIN, MultiFeature)
# register_block("Pca_visualize", "2D PCA Plot", GroupType.VISUALIZE, PCA_visualize)

register_block("svm_classifier", "Linear SVM Classifier", GroupType.CLASSIFIER, SvmClassifier)

register_block("merge_gs_platform_annotation", "Merge Gene Set with platform",
               GroupType.PROCESSING, MergeGeneSetWithPlatformAnnotation)
register_block("globaltest", "Global test", GroupType.PROCESSING, GlobalTest)
register_block("svd_agg", "Svd aggregation", GroupType.PROCESSING, SvdAggregation)
register_block("sub_agg", "Subtractive aggregation", GroupType.PROCESSING, SubAggregation)
register_block("merge_two_es", "Merge expression sets", GroupType.PROCESSING, MergeExpressionSets)

