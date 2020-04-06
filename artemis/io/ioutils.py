#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the Copyright © Her Majesty the Queen in Right of Canada, as represented by the Minister of Statistics Canada, 2019. license.

"""
IO Utility methods
Support for protocol buffers
"""
import warnings
from packaging.version import Version
from pkg_resources import parse_version

import numpy as np
from typing import Union

from artemis_externals.physt import __version__
from artemis_externals.physt.util import find_subclass
from artemis_externals.physt.histogram_base import HistogramBase
from artemis_format.pymodels.histogram_pb2 import Histogram, Meta, HistogramCollection

# Name of fields that are re-used from to_dict / from_dict
SIMPLE_CONVERSION_FIELDS = ("histogram_type", "dtype")
SIMPLE_META_KEYS = ("name", "title")

# Version writing the message
CURRENT_VERSION = __version__

# First version that should understand this
COMPATIBLE_VERSION = "0.3.42"


class VersionError(Exception):
    pass


def create_from_dict(
    data: dict, format_name: str, check_version: bool = True
) -> Union[HistogramBase, HistogramCollection]:

    """Once dict from source data is created, turn this into histogram.
    Parameters
    ----------
    data : dict

        Parsed JSON-like tree.

    Returns
    -------
    histogram : HistogramBase

        A histogram (of any dimensionality)

    """

    # Version
    if check_version:
        compatible_version = data["physt_compatible"]
        require_compatible_version(compatible_version, format_name)

    # Construction
    histogram_type = data["histogram_type"]
    if histogram_type == "histogram_collection":
        klass = HistogramCollection
    else:
        klass = find_subclass(HistogramBase, histogram_type)
    return klass.from_dict(data)


def require_compatible_version(compatible_version, word="File"):

    """Check that compatible version of input data is not too new."""
    if isinstance(compatible_version, str):
        compatible_version = parse_version(compatible_version)
    elif not isinstance(compatible_version, Version):
        raise ValueError("Type of `compatible_version` not understood.")

    current_version = parse_version(CURRENT_VERSION)
    if current_version < compatible_version:
        raise VersionError(
            "{0} written for version >= {1}, this is {2}.".format(
                word, str(compatible_version), CURRENT_VERSION
            )
        )


def write(histogram):
    """Convert a histogram to a protobuf message.

    Note: Currently, all binnings are converted to
      static form. When you load the histogram again,
      you will lose any related behaviour.

    Note: A histogram collection is also planned.
    
    Parameters
    ----------
    histogram : HistogramBase | list | dict
        Any histogram

    Returns
    -------
    message : google.protobuf.message.Message
        A protocol buffer message
    """

    histogram_dict = histogram.to_dict()
    message = Histogram()

    for field in SIMPLE_CONVERSION_FIELDS:
        setattr(message, field, histogram_dict[field])

    # Main numerical data - TODO: Optimize!
    message.frequencies.extend(histogram.frequencies.flatten())
    message.errors2.extend(histogram.errors2.flatten())

    # Binnings
    for binning in histogram._binnings:
        binning_message = message.binnings.add()
        for edges in binning.bins:
            limits = binning_message.bins.add()
            limits.lower = edges[0]
            limits.upper = edges[1]

    # All meta data
    meta_message = message.meta
    # user_defined = {}
    # for key, value in histogram.meta_data.items():
    #     if key not in PREDEFINED:
    #         user_defined[str(key)] = str(value)
    for key in SIMPLE_META_KEYS:
        if key in histogram.meta_data:
            setattr(meta_message, key, str(histogram.meta_data[key]))
    if "axis_names" in histogram.meta_data:
        meta_message.axis_names.extend(histogram.meta_data["axis_names"])

    message.physt_version = CURRENT_VERSION
    message.physt_compatible = COMPATIBLE_VERSION
    return message


def read(message):
    """Convert a parsed protobuf message into a histogram."""
    require_compatible_version(message.physt_compatible)

    # Currently the only implementation
    a_dict = _dict_from_v0342(message)
    return create_from_dict(a_dict, "Message")


def write_many(histogram_collection):
    warnings.warn("Histogram collections are unstable API. May be removed.")
    message = HistogramCollection()
    for name, histogram in histogram_collection.items():
        proto = message.histograms[name]
        proto.CopyFrom(write(histogram))
    return message
    # TODO: Will change with real HistogramCollection class


def read_many(message):
    warnings.warn("Histogram collections are unstable API. May be removed.")
    return {name: read(value) for name, value in message.histograms.items()}
    # TODO: Will change with real HistogramCollection class


def _binning_to_dict(binning_message):
    return {"bins": [[bin.lower, bin.upper] for bin in binning_message.bins]}


def _dict_from_v0342(message):
    a_dict = {key: getattr(message, key) for key in SIMPLE_CONVERSION_FIELDS}
    a_dict.update(
        {
            "physt_compatible": message.physt_compatible,
            "binnings": [_binning_to_dict(b) for b in message.binnings],
            "meta_data": {
                k: getattr(message.meta, k)
                for k in SIMPLE_META_KEYS
                if getattr(message.meta, k)
            },
        }
    )
    axis_names = list(message.meta.axis_names)
    if axis_names:
        a_dict["meta_data"].update({"axis_names": axis_names})

    print(a_dict)

    shape = [len(binning["bins"]) for binning in a_dict["binnings"]]
    a_dict.update(
        {
            "frequencies": np.asarray([f for f in message.frequencies]).reshape(shape),
            "errors2": np.asarray([e for e in message.errors2]).reshape(shape),
        }
    )
    return a_dict
