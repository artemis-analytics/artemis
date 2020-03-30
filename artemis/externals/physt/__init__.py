# The MIT License (MIT)
#
# Copyright (c) 2016-2019 Jan Pipek
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
physt
=====

P(i/y)thon h(i/y)stograms. Inspired (and based on) numpy.histogram,
but designed for humans(TM) on steroids(TM).

(C) Jan Pipek, 2016-9, MIT licence
See https://github.com/janpipek/physt

Modified by Ryan Mackenzie White for the Artemis Framework

"""
#  from . import binnings

__version__ = "0.4.6"


def histogram(data, bins=None, *args, **kwargs):
    """Facade function to create 1D histograms.

    This proceeds in three steps:
    1) Based on magical parameter bins, construct bins for the histogram
    2) Calculate frequencies for the bins
    3) Construct the histogram object itself

    *Guiding principle:* parameters understood by numpy.histogram should be
    understood also by physt.histogram as well and should result in a
    Histogram1D object with (h.numpy_bins, h.frequencies) same as the
    numpy.histogram output. Additional functionality is a bonus.

    This function is also aliased as "h1".

    Parameters
    ----------
    data : array_like, optional
        Container of all the values (tuple, list, np.ndarray, pd.Series)
    bins: int or sequence of scalars or callable or str, optional
        If iterable => the bins themselves
        If int => number of bins for default binning
        If callable => use binning method (+ args, kwargs)
        If string => use named binning method (+ args, kwargs)
    weights: array_like, optional
        (as numpy.histogram)
    keep_missed: Optional[bool]
        store statistics about how many values were lower than limits
        and how many higher than limits (default: True)
    dropna: bool
        whether to clear data from nan's before histogramming
    name: str
        name of the histogram
    axis_name: str
        name of the variable on x axis
    adaptive: bool
        whether we want the bins to be modifiable
        (useful for continuous filling of a priori unknown data)
    dtype: type
        customize underlying data type:
        default int64 (without weight) or float (with weights)

    Other numpy.histogram parameters are excluded,
    see the methods of the Histogram1D class itself.

    Returns
    -------
    physt.histogram1d.Histogram1D

    See Also
    --------
    numpy.histogram
    """
    import numpy as np
    from .histogram1d import Histogram1D, calculate_frequencies
    from .binnings import calculate_bins

    adaptive = kwargs.pop("adaptive", False)
    dtype = kwargs.pop("dtype", None)

    # Works for groupby DataSeries
    if isinstance(data, tuple) and isinstance(data[0], str):
        return histogram(data[1], bins, *args, name=data[0], **kwargs)
    elif type(data).__name__ == "DataFrame":
        raise RuntimeError(
            "Cannot create histogram " "from a pandas DataFrame. Use Series."
        )

    # Collect arguments (not to send them to binning algorithms)
    dropna = kwargs.pop("dropna", True)
    weights = kwargs.pop("weights", None)
    keep_missed = kwargs.pop("keep_missed", True)
    name = kwargs.pop("name", None)
    axis_name = kwargs.pop("axis_name", None)
    title = kwargs.pop("title", None)

    # Convert to array
    if data is not None:
        array = np.asarray(data)  # .flatten()
        if dropna:
            array = array[~np.isnan(array)]
    else:
        array = None

    # Get binning
    binning = calculate_bins(
        array,
        bins,
        *args,
        check_nan=not dropna and array is not None,
        adaptive=adaptive,
        **kwargs
    )
    # bins = binning.bins

    # Get frequencies
    if array is not None:
        (frequencies, errors2, underflow, overflow, stats) = calculate_frequencies(
            array, binning=binning, weights=weights, dtype=dtype
        )
    else:
        frequencies = None
        errors2 = None
        underflow = 0
        overflow = 0
        stats = {"sum": 0.0, "sum2": 0.0}

    # Construct the object
    if not keep_missed:
        underflow = 0
        overflow = 0
    if not axis_name:
        if hasattr(data, "name"):
            axis_name = data.name
        elif (
            hasattr(data, "fields")
            and len(data.fields) == 1
            and isinstance(data.fields[0], str)
        ):
            # Case of dask fields (examples)
            axis_name = data.fields[0]
    return Histogram1D(
        binning=binning,
        frequencies=frequencies,
        errors2=errors2,
        overflow=overflow,
        underflow=underflow,
        stats=stats,
        dtype=dtype,
        keep_missed=keep_missed,
        name=name,
        axis_name=axis_name,
        title=title,
    )


# Aliases


h1 = histogram


def collection(data, bins=10, *args, **kwargs):
    """Create histogram collection with shared binnning."""
    from physt.histogram_collection import HistogramCollection

    if hasattr(data, "columns"):
        data = {column: data[column] for column in data.columns}
    return HistogramCollection.multi_h1(data, bins, **kwargs)
