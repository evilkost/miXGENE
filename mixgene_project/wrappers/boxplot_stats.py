# -*- coding: utf-8 -*-

# Got from https://raw.github.com/matplotlib/matplotlib/3146413af636d45278fffd64de4554494d271dd9/lib/matplotlib/cbook.py

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import numpy as np


def boxplot_stats(X, whis=1.5, bootstrap=None, labels=None):
    '''
    Returns list of dictionaries of staticists to be use to draw a series of
    box and whisker plots. See the `Returns` section below to the required
    keys of the dictionary. Users can skip this function and pass a user-
    defined set of dictionaries to the new `axes.bxp` method instead of
    relying on MPL to do the calcs.

    Parameters
    ----------
    X : array-like
        Data that will be represented in the boxplots. Should have 2 or fewer
        dimensions.

    whis : float, string, or sequence (default = 1.5)
        As a float, determines the reach of the whiskers past the first and
        third quartiles (e.g., Q3 + whis*IQR, QR = interquartile range, Q3-Q1).
        Beyond the whiskers, data are considered outliers and are plotted as
        individual points. Set this to an unreasonably high value to force the
        whiskers to show the min and max data. Alternatively, set this to an
        ascending sequence of percentile (e.g., [5, 95]) to set the whiskers
        at specific percentiles of the data. Finally, can  `whis` be the
        string 'range' to force the whiskers to the min and max of the data.
        In the edge case that the 25th and 75th percentiles are equivalent,
        `whis` will be automatically set to 'range'

    bootstrap : int or None (default)
        Number of times the confidence intervals around the median should
        be bootstrapped (percentile method).

    labels : sequence
        Labels for each dataset. Length must be compatible with dimensions
        of `X`

    Returns
    -------
    bxpstats : A list of dictionaries containing the results for each column
        of data. Keys of each dictionary are the following:

        ========   ===================================
        Key        Value Description
        ========   ===================================
        label      tick label for the boxplot
        mean       arithemetic mean value
        median     50th percentile
        q1         first quartile (25th percentile)
        q3         third quartile (75th percentile)
        cilo       lower notch around the median
        ciho       upper notch around the median
        whislo     end of the lower whisker
        whishi     end of the upper whisker
        fliers     outliers
        ========   ===================================

    Notes
    -----
    Non-bootstrapping approach to confidence interval uses Gaussian-based
    asymptotic approximation:

    .. math:: \mathrm{med} \pm 1.57 \times \frac{\mathrm{iqr}}{\sqrt{N}}

    General approach from:
    McGill, R., Tukey, J.W., and Larsen, W.A. (1978) "Variations of
        Boxplots", The American Statistician, 32:12-16.

    '''

    def _bootstrap_median(data, N=5000):
        # determine 95% confidence intervals of the median
        M = len(data)
        percentiles = [2.5, 97.5]

        ii = np.random.randint(M, size=(N, M))
        bsData = x[ii]
        estimate = np.median(bsData, axis=1, overwrite_input=True)

        CI = np.percentile(estimate, percentiles)
        return CI

    def _compute_conf_interval(data, med, iqr, bootstrap):
        if bootstrap is not None:
            # Do a bootstrap estimate of notch locations.
            # get conf. intervals around median
            CI = _bootstrap_median(data, N=bootstrap)
            notch_min = CI[0]
            notch_max = CI[1]
        else:

            N = len(data)
            notch_min = med - 1.57 * iqr / np.sqrt(N)
            notch_max = med + 1.57 * iqr / np.sqrt(N)

        return notch_min, notch_max

    # output is a list of dicts
    bxpstats = []

    # convert X to a list of lists
    if hasattr(X, 'shape'):
        # one item
        if len(X.shape) == 1:
            if hasattr(X[0], 'shape'):
                X = list(X)
            else:
                X = [X, ]

        # several items
        elif len(X.shape) == 2:
            nrows, ncols = X.shape
            if nrows == 1:
                X = [X[0]]  # TODO: create patch request for matplotlib
            elif ncols == 1:
                X = [X.ravel()]
            else:
                X = [X[:, i] for i in xrange(ncols)]
        else:
            raise ValueError("input `X` must have 2 or fewer dimensions")

    if not hasattr(X[0], '__len__'):
        X = [X]

    ncols = len(X)
    if labels is None:
        labels = [str(i) for i in range(ncols)]
    elif len(labels) != ncols:
        raise ValueError("Dimensions of labels and X must be compatible")

    for ii, (x, label) in enumerate(zip(X, labels), start=0):
        # empty dict
        stats = {}
        stats['label'] = label

        # arithmetic mean
        stats['mean'] = np.mean(x)

        # medians and quartiles
        q1, med, q3 = np.percentile(x, [25, 50, 75])

        # interquartile range
        stats['iqr'] = float(q3 - q1)
        if stats['iqr'] == 0:
            whis = 'range'

        # conf. interval around median
        stats['cilo'], stats['cihi'] = map(float, _compute_conf_interval(
            x, med, stats['iqr'], bootstrap
        ))

        # lowest/highest non-outliers
        if np.isscalar(whis):
            if np.isreal(whis):
                loval = q1 - whis * stats['iqr']
                hival = q3 + whis * stats['iqr']
            elif whis in ['range', 'limit', 'limits', 'min/max']:
                loval = np.min(x)
                hival = np.max(x)
            else:
                whismsg = ('whis must be a float, valid string, or '
                           'list of percentiles')
                raise ValueError(whismsg)
        else:
            loval = np.percentile(x, whis[0])
            hival = np.percentile(x, whis[1])

        # get high extreme
        wiskhi = np.compress(x <= hival, x)
        if len(wiskhi) == 0 or np.max(wiskhi) < q3:
            stats['whishi'] = float(q3)
        else:
            stats['whishi'] = float(np.max(wiskhi))

        # get low extreme
        wisklo = np.compress(x >= loval, x)
        if len(wisklo) == 0 or np.min(wisklo) > q1:
            stats['whislo'] = float(q1)
        else:
            stats['whislo'] = float(np.min(wisklo))

        # compute a single array of outliers
        stats['fliers'] = list(np.hstack([
            np.compress(x < stats['whislo'], x),
            np.compress(x > stats['whishi'], x)
        ]))

        # add in teh remaining stats and append to final output
        stats['q1'], stats['med'], stats['q3'] = map(float, [q1, med, q3])
        bxpstats.append(stats)

    return bxpstats