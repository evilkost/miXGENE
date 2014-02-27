# -*- coding: utf-8 -*-
import traceback
from celery import task

import numpy as np
import pandas as pd

from pandas import DataFrame, Series, Index

# load csv data
# mRNA
# m_rna = pd.read_csv('data/mRNA.csv', header=0, index_col=0).transpose()
# miRNA
# mi_rna = pd.read_csv('data/miRNA.csv', header=0, index_col=0).transpose()
# phenotype
# pt = pd.read_csv('data/phenotype.csv', header=0, index_col=0)
# reponse
# response = pt['sex']=='M'
import sys


#@task(name="wrappers.aggregation.aggregation_task")
def aggregation_task(exp, block,
                     mode, c,
                     m_rna_es, mi_rna_es, interaction_matrix,
                     base_filename,
    ):
    """
        @type m_rna_es: ExpressionSet
        @type mi_rna_es: ExpressionSet
        @type interaction_matrix: BinaryInteraction

    """

    if mode == "SVD":
        agg_func = svd_agg
    elif mode == "SUB":
        agg_func = sub_agg

    m_rna = m_rna_es.get_assay_data_frame()
    mi_rna = mi_rna_es.get_assay_data_frame()
    targets_matrix = interaction_matrix.load_matrix()

    result_df = agg_func(m_rna, mi_rna, targets_matrix, c)
    result = m_rna_es.clone(base_filename)
    result.store_assay_data_frame(result_df)
    result.store_pheno_data_frame(mi_rna_es.get_pheno_data_frame())

    return [result], {}


### umela data

m_rna = DataFrame([[1,2,3],[1.5,0.5,2]], index=['s1', 's2'], columns=['m1', 'm2', 'm3'])
mi_rna = DataFrame([[0.5, 0.7, 0.1],[1,4,2]], index=['s1', 's2'], columns=['u1', 'u2', 'u3'])
targets_matrix = DataFrame([[1, 1, 0],[1, 1, 0], [0, 0, 1]], index=['u1', 'u2', 'u3'], columns=['m1', 'm2', 'm3'])

# sub agg
def sub_agg(m_rna, mi_rna, targets_matrix, c=1):
    #
    targeting_miRNAs = targets_matrix.sum(axis=0)
    aggtd_data =   m_rna.copy()
    # for all relevant miRNA labels
    miRNA_labels = set(mi_rna.columns) & set(targets_matrix.index)
    #
    for i in Index(miRNA_labels):
        i = Index([i])
        targets = targets_matrix.ix[i, targets_matrix.xs(i[0])==1].columns
        #
        ratios = m_rna[targets].apply(lambda x:[i*1.0/sum(x) for i in x], axis=1)
        subtracts = ratios.apply(lambda x: x*mi_rna[i[0]], axis=0) # ratios*c*miRNA sample-wise
        subtracts = subtracts.apply(lambda x:x/targeting_miRNAs[targets] , axis=1)
        aggtd_data[subtracts.columns] = aggtd_data[subtracts.columns]-subtracts
    return aggtd_data

# svd agg
def svd_agg(m_rna, mi_rna, targets_matrix, c=1):
    #
    mRNA_data = m_rna.apply(lambda x: 1.0*x/max(x), axis=0)
    miRNA_data = mi_rna.apply(lambda x: 1-1.0*x/max(x), axis=0)
    #
    aggregate_data = mRNA_data
    #
    common_mRNAs =  Index(set(mRNA_data.columns) & set(targets_matrix.columns))
    common_miRNAs = Index(set(miRNA_data.columns) & set(targets_matrix.index))
    #
    for mRNA in common_mRNAs:
        #
        mRNA = Index([mRNA])
        #
        targetting_miRNAs = targets_matrix.ix[targets_matrix[mRNA[0]]==1, mRNA].index
        #
        selected_miRNA = miRNA_data.ix[:, targetting_miRNAs]
        #
        if len(selected_miRNA.columns)>1:
            first_comp = DataFrame(np.linalg.svd(selected_miRNA)[2]).ix[0, :]
            first_comp.index = selected_miRNA.index
        new_rep = DataFrame(np.linalg.svd(DataFrame([aggregate_data.ix[:,mRNA[0]], first_comp ]).transpose())[2]).ix[0, :]
        new_rep.index = aggregate_data.index
        aggregate_data.ix[:, mRNA[0]] = new_rep
    return aggregate_data

def svd_agg_train(m_rna, mi_rna, targets_matrix, hide_columns=Index([])):
    #
    sample_indexes = m_rna.index - hide_columns
    mRNA_data = m_rna.apply(lambda x: 1.0*x/max(x), axis=0).ix[sample_indexes, :]
    miRNA_data = mi_rna.apply(lambda x: 1-1.0*x/max(x), axis=0).ix[sample_indexes, :]
    #
    aggregate_data = mRNA_data
    #
    common_mRNAs =  Index(set(mRNA_data.columns) & set(targets_matrix.columns))
    common_miRNAs = Index(set(miRNA_data.columns) & set(targets_matrix.index))
    #
    for mRNA in common_mRNAs:
        #
        mRNA = Index([mRNA])
        #
        targetting_miRNAs = targets_matrix.ix[targets_matrix[mRNA[0]]==1, mRNA].index
        #
        selected_miRNA = miRNA_data.ix[:, targetting_miRNAs]
        #
        if len(selected_miRNA.columns)>1:
            first_comp = DataFrame(np.linalg.svd(selected_miRNA)[2]).ix[0, :]
            first_comp.index = selected_miRNA.index
        new_rep = DataFrame(np.linalg.svd(DataFrame([aggregate_data.ix[:,mRNA[0]], first_comp ]).transpose())[2]).ix[0, :]
        new_rep.index = aggregate_data.index
        aggregate_data.ix[:, mRNA[0]] = new_rep
    return aggregate_data


# print(sub_agg(m_rna, mi_rna, targets_matrix))

if __name__ == "__main__":
    print(svd_agg(m_rna, mi_rna, targets_matrix))