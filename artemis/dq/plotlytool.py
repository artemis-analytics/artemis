"""
#! ~/miniconda3/envs/artemis-dev/bin/python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.

:author: Mitchell Shahen (mitchell.shahen2@canada.ca)

:history: Oct 3, 2019

This module contains five class objects performing various functions to:
    - Extract histogram and TDigest data from a metastore object.
    - Create dictionaries describing histogram plotting properties.
    - Create dictionaries describing TDigest CDF plotting properties.
    - Create Plotly figures from dictionaries of plotting properties.
    - Organize, save, and/or plot the Plotly figures.

Module Structure:

ProcessHist(histograms=None):
    - _create_dict(histogram=None, name="", address="")
    - _get_hist_obj(histogram=None)
    - _validate(histograms=None)
    - generate_collection(histogram=None, valid_name="", address="")
    - generate_traces()

ProcessTDigest(tdigests=None):
    - _calculate_cdf(tdigest=None, method="")
    - _create_dict(data=None, name="", address="")
    - _get_digest_map(tdigest=None)
    - _validate(tdigests=None)
    - get_centroids(digest_map=None)
    - generate_traces()

MergeHist(traces=None, max_cols=0):
    - _validate(traces=None, max_cols=0)
    - combine(traces=None, names=None)
    - modify_coord(traces=None, max_cols=0)
    - modify_colours(traces=None)
    - merge()

BuildFigure(traces=None, figure_type=""):
    - _create_bar(traces=None, template=None)
    - _create_scatter(traces=None, template=None)
    - _validate(traces=None, figure_type="")
    - update_figure(figure=None, figure_type="")
    - generate_figure()

PlotlyTool(store=None, uuid=""):
    - _check_output(output="", check=True)
    - _list(store=None, uuid="")
    - _validate(store=None, uuid="")
    - get_figure(traces=None, output="", show=True, check=True, fig_type="")
    - visualize(output="", show=True, check=True)

This tool's intended functionality includes extracting Histograms and TDigests from a
dataset, locatable by a UUID code, within an input store, then saving, and possibly
plotting, the histograms and TDigest CDFs as HTML files.

This can be done by including the following code,

```
from artemis.dq.plotlytool import PlotlyTool
PlotlyTool(
    store=my_store,
    uuid=dataset_uuid
).visualize(
    output=path_to_directory,
    show=show_plots,
    check=check_with_user
)
```

Note that each of `my_store`, `dataset_uuid`, `path_to_directory`, `show_plots`, and
`check_with_user` are defined as necessary.

Created in collaboration with:

:collaborators:
Ryan White (ryan.white4@canada.ca)
Dominic Parent (dominic.parent@canada.ca),
William Fairgrieve (william.fairgrieve@canada.ca)
Russell Gill (russell.gill@canada.ca)
"""

# ---------- PYLINT DISABLE PARAMETERS ---------- #

# pylint: disable=bare-except
# pylint: disable=c-extension-no-member
# pylint: disable=no-member
# pylint: disable=protected-access
# pylint: disable=too-many-branches
# pylint: disable=too-many-lines
# pylint: disable=too-many-locals

# ---------- IMPORT NECESSARY PACKAGES ---------- #

# Standard Import(s)
import logging
import os
import time
import urllib.parse

# External Import(s)
import google
import numpy
from scipy import interpolate

# Plotly Import(s)
from plotly.colors import DEFAULT_PLOTLY_COLORS
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from plotly.offline import plot as plot_save

# Local Artemis Import(s)
from artemis.core.book import TDigestBook
from artemis.externals.tdigest.tdigest import TDigest
from artemis.io.protobuf import cronus_pb2, histogram_pb2
from artemis.meta.cronus import BaseObjectStore

# ---------- GLOBAL PARAMETERS ---------- #

# for each trace name provided, create a subplot containing only
# the traces with names that contain the provided trace name
REQ_HIST_TRACE_NAMES = ["all"]

# each trace specified in this parameter is included in
# the calculated, outputted, and rendered tdigest plots
REQ_TDIGEST_TRACE_NAMES = ["all"]

# set the maximum number of columns in the histogram subplot(s)
MAX_HIST_SUBPLOT_COLUMNS = 2

# set the maximum number of columns in the TDigest subplot(s)
MAX_TDIGEST_SUBPLOT_COLUMNS = 2

# set the type of CDF analysis to be conducted
CDF_ANALYSIS_METHOD = "spline"

# ---------- TRACE TEMPLATES ---------- #

BAR_TEMPLATE = {
    "name": "",
    "visible": True,
    "showlegend": True,
    "legendgroup": "",
    "opacity": 1,
    "ids": None,
    "x": None,
    "x0": 0,
    "dx": 1,
    "y": None,
    "y0": 0,
    "dy": 1,
    "base": None,
    "width": None,
    "offset": None,
    "text": "",
    "textposition": None,
    "hovertext": "",
    "hoverinfo": "all",
    "hovertemplate": "",
    "meta": None,
    "customdata": None,
    "xaxis": "x",
    "yaxis": "y",
    "orientation": "v",
    "alignmentgroup": "",
    "offsetgroup": "",
    "marker": {
        "line": {
            "width": 0,
            "color": None,
            "cauto": True,
            "cmin": None,
            "cmax": None,
            "cmid": None,
            "colorscale": None,
            "autocolorscale": True,
            "reversescale": None,
            "coloraxis": None,
        },
        "color": None,
        "cauto": True,
        "cmin": None,
        "cmax": None,
        "cmid": None,
        "colorscale": None,
        "autocolorscale": True,
        "reversescale": None,
        "showscale": None,
        "colorbar": {
            "thicknessmode": "pixels",
            "thickness": 30,
            "lenmode": "fraction",
            "len": 1,
            "x": 1.02,
            "xanchor": "left",
            "xpad": 10,
            "y": 0.5,
            "yanchor": "middle",
            "ypad": 10,
            "outlinecolor": "#444",
            "outlinewidth": 1,
            "bordercolor": "#444",
            "borderwidth": 0,
            "bgcolor": "rgba(0,0,0,0)",
            "tickmode": None,
            "nticks": 0,
            "tick0": None,
            "dtick": None,
            "tickvals": None,
            "ticktext": None,
            "ticks": "",
            "ticklen": 5,
            "tickwidth": 1,
            "tickcolor": "#444",
            "showticklabels": True,
            "tickfont": {"family": None, "size": None, "color": None},
            "tickangle": 0,
            "tickformat": "",
            "tickprefix": "",
            "showtickprefix": "all",
            "ticksuffix": "",
            "showticksuffix": "all",
            "separatethousands": None,
            "exponentformat": "B",
            "showexponent": "all",
            "title": {
                "text": None,
                "font": {"family": None, "size": None, "color": None},
                "side": "top",
            },
        },
        "coloraxis": None,
        "opacity": 1,
    },
    "textangle": 0,
    "textfont": {"family": None, "size": None, "color": None},
    "error_x": {
        "visible": False,
        "type": None,
        "symmetric": None,
        "array": None,
        "arrayminus": None,
        "value": 10,
        "valueminus": 10,
        "traceref": 0,
        "tracerefminus": 0,
        "copy_ystyle": None,
        "color": None,
        "thickness": 2,
        "width": None,
    },
    "error_y": {
        "visible": False,
        "type": None,
        "symmetric": None,
        "array": None,
        "arrayminus": None,
        "value": 10,
        "valueminus": 10,
        "traceref": 0,
        "tracerefminus": 0,
        "color": None,
        "thickness": 2,
        "width": None,
    },
    "selected": {
        "marker": {"opacity": None, "color": None},
        "textfont": {"color": None},
    },
    "unselected": {
        "marker": {"opacity": None, "color": None},
        "textfont": {"color": None},
    },
    "cliponaxis": True,
    "constraintext": "both",
    "hoverlabel": {
        "bgcolor": None,
        "bordercolor": None,
        "font": {"family": None, "size": None, "color": None},
        "align": "auto",
        "namelength": 15,
    },
    "insidetextanchor": "end",
    "insidetextfont": {"family": None, "size": None, "color": None},
    "outsidetextfont": {"family": None, "size": None, "color": None},
    "xcalendar": "gregorian",
    "ycalendar": "gregorian",
    "uirevision": None,
}
SCATTER_TEMPLATE = {
    "name": "",
    "visible": True,
    "showlegend": True,
    "legendgroup": "",
    "opacity": 1,
    "mode": None,
    "ids": None,
    "x": None,
    "x0": 0,
    "dx": 1,
    "y": None,
    "y0": 0,
    "dy": 1,
    "text": "",
    "textposition": None,
    "hovertext": "",
    "hoverinfo": "all",
    "hovertemplate": "",
    "meta": None,
    "customdata": None,
    "xaxis": "x",
    "yaxis": "y",
    "orientation": "v",
    "groupnorm": "",
    "stackgroup": "",
    "marker": {
        "symbol": "circle",
        "opacity": None,
        "size": 6,
        "maxdisplayed": 0,
        "sizeref": 1,
        "sizemin": 0,
        "sizemode": "diameter",
        "line": {
            "width": 0,
            "color": None,
            "cauto": True,
            "cmin": None,
            "cmax": None,
            "cmid": None,
            "colorscale": None,
            "autocolorscale": True,
            "reversescale": None,
            "coloraxis": None,
        },
        "gradient": {"type": "none", "color": None},
        "color": None,
        "cauto": True,
        "cmin": None,
        "cmax": None,
        "cmid": None,
        "colorscale": None,
        "autocolorscale": True,
        "reversescale": None,
        "showscale": None,
        "colorbar": {
            "thicknessmode": "pixels",
            "thickness": 30,
            "lenmode": "fraction",
            "len": 1,
            "x": 1.02,
            "xanchor": "left",
            "xpad": 10,
            "y": 0.5,
            "yanchor": "middle",
            "ypad": 10,
            "outlinecolor": "#444",
            "outlinewidth": 1,
            "bordercolor": "#444",
            "borderwidth": 0,
            "bgcolor": "rgba(0,0,0,0)",
            "tickmode": None,
            "nticks": 0,
            "tick0": None,
            "dtick": None,
            "tickvals": None,
            "ticktext": None,
            "ticks": "",
            "ticklen": 5,
            "tickwidth": 1,
            "tickcolor": "#444",
            "showticklabels": True,
            "tickfont": {"family": None, "size": None, "color": None},
            "tickangle": None,
            "tickformat": "",
            "tickprefix": "",
            "showtickprefix": "all",
            "ticksuffix": "",
            "showticksuffix": "all",
            "separatethousands": None,
            "exponentformat": "B",
            "showexponent": "all",
            "title": {
                "text": None,
                "font": {"family": None, "size": None, "color": None},
                "side": "top",
            },
        },
        "coloraxis": None,
    },
    "line": {
        "color": None,
        "width": 2,
        "shape": "linear",
        "smoothing": 1,
        "dash": "solid",
        "simplify": True,
    },
    "textfont": {"family": None, "size": None, "color": None},
    "error_x": {
        "visible": False,
        "type": None,
        "symmetric": None,
        "array": None,
        "arrayminus": None,
        "value": 10,
        "valueminus": 10,
        "traceref": 0,
        "tracerefminus": 0,
        "copy_ystyle": None,
        "color": None,
        "thickness": 2,
        "width": None,
    },
    "error_y": {
        "visible": False,
        "type": None,
        "symmetric": None,
        "array": None,
        "arrayminus": None,
        "value": 10,
        "valueminus": 10,
        "traceref": 0,
        "tracerefminus": 0,
        "color": None,
        "thickness": 2,
        "width": None,
    },
    "selectedpoints": None,
    "selected": {
        "marker": {"opacity": None, "color": None, "size": None},
        "textfont": {"color": None},
    },
    "unselected": {
        "marker": {"opacity": None, "color": None, "size": None},
        "textfont": {"color": None},
    },
    "cliponaxis": True,
    "connectgaps": None,
    "fill": None,
    "fillcolor": None,
    "hoverlabel": {
        "bgcolor": None,
        "bordercolor": None,
        "font": {"family": None, "size": None, "color": None},
        "align": "auto",
        "namelength": 15,
    },
    "hoveron": None,
    "stackgaps": "infer zero",
    "xcalendar": "gregorian",
    "ycalendar": "gregorian",
    "uirevision": None,
}

# ---------- DEFINE CLASS OBJECTS ---------- #


class ProcessHist:
    """
    Class to create dictionaries of plotting properties from input histogram objects.

    Parameters
    --------------------
    `histograms`: `google.protobuf.pyext._message.RepeatedCompositeContainer`
        A Cronus protobuf object containing histogram data to be plotted.
    """

    def __init__(self, histograms=None):
        """
        Constructor object for `ProcessHist` to pass parameters to other methods.
        """

        self.histograms = histograms

    @staticmethod
    def _create_dict(histogram=None, name="", address=""):
        """
        Creates a dictionary containing plotting instructions.
        The instructions specify parameters to be interpretted
        when plotting the histogram. In addition to the histogram object,
        the histogram's name and location on the user's computer is made
        available to the user when
        viewing the plot, useful for differentiating between plotted histograms.

        Parameters
        --------------------
        `histogram`: `histogram_pb2.Histogram`
            A `Histogram` object containing data and plotting information.
        `name`: `str`
            The name of the histogram dataset.
        `address`: `str`
            Location of the Cronus object originally containing the histogram's data.

        Returns
        --------------------
        `trace`: `dict`
            Dictionary containing the histogram data and its plotting information.
        """

        # extract the data to be plotted and the necessary binning information
        frequencies = [int(frequency) for frequency in histogram.frequencies]
        binnings = [[bin.lower, bin.upper] for bin in histogram.binnings[0].bins]

        # define lists of the midpoint and width of each bin, respectively
        bin_means = [(binning[0] + binning[1]) / 2 for binning in binnings]
        width = [binning[1] - binning[0] for binning in binnings]

        # decompose the address extracting it Unique ID and Job ID
        filename = address.split("/")[-1]
        components = filename.split(".")
        uuid = components[0]
        job_id = components[2]

        # define text to be visible when hovering over a data point
        text = [
            "<br>UUID: {}<br>Job ID: {}<br>Bin: {}-{}<br>Freq: {}".format(
                uuid,  # the dataset's unique identifier, same for all bars in the trace
                job_id,  # job uuid that created the dataset, same for all bars in trace
                item[0],  # the start of the current bin
                item[1],  # the end of the current bin
                frequencies[i],  # the number of data points in the current bin
            )
            for i, item in enumerate(binnings)
        ]

        # define the dictionary containing the histogram plotting information
        trace_data = {
            "name": name,
            "x": bin_means,
            "y": frequencies,
            "width": width,
            "text": text,  # include the text, text is visible when hovering over a bar
            "marker": {"color": "black"},  # set a default colour
        }

        # assign the plotting information as item in a secondary dictionary along with
        # some default row and column coordinates and the type of plot to be created
        trace = {"data": trace_data, "row": 1, "col": 1, "plot_type": "Bar"}

        return trace

    @staticmethod
    def _get_hist_obj(histogram=None):
        """
        A class method to extract histogram data using an input Cronus object.
        The Cronus object contains the address of a histogram dataset.
        The dataset is located using the address and loaded
        into a `HistogramCollection` object.

        Parameters
        --------------------
        `histogram`: `cronus_pb2.CronusObject`
            A Cronus protobuf object containing histogram data and information.

        Returns
        --------------------
        `output`: `tuple`
            A tuple of a histogram object containing the histogram data from the Cronus
            object, as a `HistogramCollection`, and the address of the Cronus object.
        """

        # define an empty histogram collection object to contain the histogram data
        new_histogram = histogram_pb2.HistogramCollection()

        # get the address of the histogram Cronus object, correct invalid characters
        address = histogram.address
        address = address.replace("file://", "")
        address = address.replace("%40", "@")

        if os.path.exists(address):
            # open the file and add the data to the new histogram
            with open(address, "rb") as open_file:
                new_histogram.ParseFromString(open_file.read())
        else:
            logging.error("Invalid Cronus Histogram Address: '%s'.", address)
            logging.info(
                "Cronus histogram data could not be located. "
                "Therefore, no histogram data is available."
            )

        output = new_histogram, address

        return output

    @staticmethod
    def _validate(histograms=None):
        """
        Class method to validate the `histograms` parameter.
        The histogram container and interior histograms are checked.
        If the container is of an incoorect type, its histograms cannot
        be extracted. If the histograms are of the incorrect type,
        they cannot be used moving forward.
        In both cases, an empty list is returned rather than a list of histograms.

        Parameters
        --------------------
        `histograms`: `google.protobuf.pyext._message.RepeatedCompositeContainer`
            A Cronus object containing several histogram datasets.

        Returns
        --------------------
        `valid_histograms`: `list`
            List of histograms from `histograms` that are of the proper type.
        """

        # check the type of container that was provided;
        # if invalid, provide no histograms
        if not isinstance(
            histograms, google.protobuf.pyext._message.RepeatedCompositeContainer
        ):
            logging.error("Invalid Histogram Container Type: '%s'.", type(histograms))
            logging.info(
                "The container holding histogram data was expected to be a "
                "`google.protobuf.pyext._message.repeatedCompositeContainer. "
                "Therefore, there is no histogram data available to plot."
            )
            histograms = []

        # define an empty list to contain the validated histograms
        valid_histograms = []

        # check if each input histogram is of the proper type; if so, pass it to a list
        for histogram in histograms:
            if not isinstance(histogram, cronus_pb2.CronusObject):
                logging.error("Invalid Histogram Type: `%s`.", type(histogram))
                logging.info(
                    "A histogram object from input container is of an invalid type. "
                    "The data from this object is not available for plotting."
                )
            else:
                valid_histograms.append(histogram)

        # if no histograms were determined to be valid, provide a `None` value instead
        if not valid_histograms:
            valid_histograms = None

            # if the valid histograms was an empty list, log why no data was available
            if isinstance(valid_histograms, list):
                logging.error("No Valid Histogram Data Available.")
                logging.info(
                    "Each histogram from the input container was found to be invalid. "
                    "Therefore, there is no histogram data available for plotting."
                )

        return valid_histograms

    def generate_collection(self, histogram=None, valid_name="", address=""):
        """
        Generate list of histogram trace objects from input `HistogramCollection`.
        Each histogram in the collection whose name contains `valid_name`
        is extracted and passed to `_create_dict`.
        From `_create_dict`, a dictionary of plotting properties is produced,
        which is added to a list. Once the list is populated
        with the dictionaries of all
        histograms containing `valid_name`, the list is returned.

        Parameters
        --------------------
        `histogram`: `histogram_pb2.HistogramCollection`
            `HistogramCollection` object of every histogram from initial Cronus object.
        `valid_name`: `str`
            Only traces whose name contains this parameter will be plotted.
        `address`: `str`
            The location of Cronus object containing the histogram's data.

        Returns
        --------------------
        `all_traces`: `list`
            A list of all the traces from `histograms` whose name contains
            the `valid_name` parameter, each converted to dictionaries.
        """

        # create an empty list of traces and set the default subplot coordinates
        all_traces = []

        for name in histogram.histograms:
            # ensure the `valid_name` identifier is present
            if valid_name in name.lower():
                # if the identifier is found, extract the histogram
                single_hist = histogram.histograms[name]

                # try to access the histogram data and create a trace dictionary
                try:
                    # if the histogram contains at least one non-zero data point,
                    # convert the data to a dictionary and add it to the trace list
                    if bool(float(max(single_hist.frequencies)) > 0.0):
                        trace_dict = self._create_dict(
                            histogram=single_hist, name=name, address=address
                        )
                        all_traces.append(trace_dict)
                    else:
                        logging.error("No Valid Data in Histogram: '%s'.", name.upper())
                        logging.info(
                            "Histogram trace only contains data with a value of 0. "
                            "Therefore, the trace will not be created."
                        )
                except ValueError:
                    logging.error("No Data Found in Histogram: '%s'.", name.upper())
                    logging.info(
                        "This histogram trace is empty or contains no data. "
                        "Therefore, the trace will not be created."
                    )

        return all_traces

    def generate_traces(self):
        """
        Generate a list of lists each containing histogram trace objects originating
        from the histograms container, `histograms`, passed to the class object.
        Only histograms that have been requested by name through REQ_HIST_TRACE_NAMES
        variable will have plotting dictionaries created and added to the list of lists.
        Each dictionary in a sub-list contains plotting information
        from the same histogram object. Therefore, if the input container
        contains only one histogram collection object,
        the output of this method will be a list with one element,
        a list of plotting dictionaries produced using the requested histograms
        from the single Histogram collection.
        If the list of requested histograms is invalid,
        all available histograms are used.

        Returns
        --------------------
        `traces`: `list`
            List of dictionaries containing the traces of the inputted histograms.
        """

        # extract the validated histograms from the original `histograms` object
        histograms = self._validate(histograms=self.histograms)

        # get the list of valid histogram traces names, validate it is a list
        if isinstance(REQ_HIST_TRACE_NAMES, list):
            req_hist_names = REQ_HIST_TRACE_NAMES
        elif isinstance(REQ_HIST_TRACE_NAMES, str):
            req_hist_names = [REQ_HIST_TRACE_NAMES]
        else:
            logging.error(
                "Invalid Histogram Trace Names Variable Type: '%s'.",
                type(REQ_HIST_TRACE_NAMES),
            )
            logging.info(
                "The variable of valid histogram trace names expected list "
                "Therefore, every available histogram trace will be created."
            )
            req_hist_names = ["all"]

        # define a list to contain all the traces
        all_traces = []

        if histograms:
            for iteration, histogram in enumerate(histograms):
                # extract the histogram object from the Cronus object
                histogram, address = self._get_hist_obj(histogram=histogram)

                logging.info("Creating Histogram Trace %s...", int(iteration) + 1)

                # list the keywords from all the histgrams in the histogram collection
                all_hist_names = [
                    name.lower().split(".") for name in histogram.histograms
                ]
                if not all_hist_names:
                    logging.error("Detected Empty Histogram.")
                    logging.info(
                        "A histogram object contains no histogram data. "
                        "Therefore, no trace can be created."
                    )

                # make a list of all the keywords used in the names of the histograms
                all_hist_keywords = []
                for list1 in all_hist_names:
                    for item in list1:
                        all_hist_keywords.append(item)

                # only include valid requested trace keywords
                trace_names = []
                for item in req_hist_names:
                    if item.lower() in ["", "all"]:
                        trace_names.append("")
                    elif item.lower() not in all_hist_keywords:
                        logging.error("Invalid Requested Histogram Name: '%s'.", item)
                        logging.info(
                            "No histograms named with above keyword could be found. "
                            "Therefore, the requested data cannot be plotted."
                        )
                    else:
                        trace_names.append(item)

                # verify there are valid traces
                if not trace_names:
                    logging.error("No Valid Traces Found.")
                    logging.warning(
                        "All the requested histogram traces could not be found. "
                        "Therefore, no histograms will be created and plotted."
                    )

                # create the list of trace(s) depending on the type of the histogram
                for name in trace_names:
                    logging.info("Creating Histogram Collection...")
                    traces = self.generate_collection(
                        histogram=histogram, valid_name=name, address=address
                    )
                    if len(traces) != 0:
                        all_traces.append(traces)

        return all_traces


class ProcessTDigest:
    """
    Class to create dictionaries describing TDigest data to be plotted.
    The methods in this class load TDigest datasets, produce TDigest maps,
    extract centroids, compute CDFs, and generate dictionaries of plotting instructions.

    Parameters
    --------------------
    `tdigests`: `google.protobuf.pyext._message.RepeatedCompositeContainer`
        A protobuf container object containing TDigests to be analyzed.
    """

    def __init__(self, tdigests=None):
        """
        Constructor class method to pass arguments to other class methods.
        """

        self.tdigests = tdigests

    @staticmethod
    def _calculate_cdf(tdigest=None, method=""):
        """
        Method to calculate the x-axis and y-axis data for a CDF plot.
        Percentile markers from the inputted TDigest object are used as x-axis values
        and are supplied to the TDigest's cdf property to generate the y-axis values.
        Various methods can be used to mainuplate the x-axis percentile values and the
        y-axis CDF values.
        The `method` parameter is used to indicate which analysis method to use, if any.

        Parameters
        --------------------
        `tdigest`: `artemis.externals.tdigest.tdigest.TDigest`
            TDigest object containing data used to calculate and plot CDF
        `method`: `str`
            The type of analysis method used to generate the CDF data.

        Returns
        --------------------
        `x_data, y_data`: `tuple`
            A tuple of two lists of data representing x-axis and y-axis of a CDF.
        """

        # verify the input TDigest is, in fact, a TDigest object
        if not isinstance(tdigest, TDigest):
            logging.error("Invalid TDigest Object Type: '%s'.", type(tdigest))
            logging.info(
                "The inputted dataset was expected to be a TDigest object. "
                "Therefore, a CDF cannot be calculated."
            )
            method = None

        # define the analysis method used to create the CDF; check if it's valid
        if isinstance(method, str):
            method = method.lower()
        if method not in ["percentiles", "granular", "uniform", "spline"]:
            logging.error("Invalid CDF Analysis Method: '%s'.", method)
            logging.info(
                "Supported CDF analysis methods include: "
                "Percentiles, Granular, Uniform, and Spline."
            )

        # define default x_data and y_data variables
        x_data = None
        y_data = None

        if method == "percentiles":
            # create the CDF of the current TDigest data object using percentiles
            percentiles = []
            cdf_percentiles = []
            for i in range(101):
                percentiles.append(tdigest.percentile(i))
                cdf_percentiles.append(tdigest.cdf(percentiles[i]))

            x_data = percentiles
            y_data = cdf_percentiles

        if method == "granular":
            # create the CDF based on using more fine-tuned percentiles
            granular_x = []
            granular_cdf = []
            for i in range(1001):
                granular_x.append(tdigest.percentile(i / 10))
                granular_cdf.append(tdigest.cdf(granular_x[i]))

            x_data = granular_x
            y_data = granular_cdf

        if method == "uniform":
            # create the CDF using uniformly placed x-coordinate values
            uniform_x = numpy.linspace(
                start=tdigest.percentile(0), stop=tdigest.percentile(100), num=100
            )
            uniform_cdf = []
            for __, x_val in enumerate(uniform_x):
                uniform_cdf.append(tdigest.cdf(x_val))

            x_data = uniform_x
            y_data = uniform_cdf

        if method == "spline":
            # create the CDF using spline interpolation
            uniform_x = numpy.linspace(
                start=tdigest.percentile(0), stop=tdigest.percentile(100), num=100
            )
            uniform_cdf = []
            for __, x_val in enumerate(uniform_x):
                uniform_cdf.append(tdigest.cdf(x_val))

            tck = interpolate.splrep(uniform_x, uniform_cdf)
            cdf_spline = interpolate.splev(uniform_x, tck, der=0)

            x_data = uniform_x
            y_data = cdf_spline

        return x_data, y_data

    @staticmethod
    def _create_dict(data=None, name="", address=""):
        """
        Method to create a dictionary of plotting instructions.
        The instructions include data to plot, the type of plot, the colour scheme,
        and accompanying text.

        Parameters
        --------------------
        `data`: `numpy.ndarray`
            TDigest data intended to be plotted or included in the plot.
        `name`: `str`
            The intended name of the data being plotted.
        `address`: `str`
            The location of the Cronus object initially containing the TDigest data.

        Returns
        --------------------
        `trace`: `dict`
            A dictionary of centroid data and plotting properties.
        """

        # get data from the input dataset
        x_data = data[0]
        y_data = data[1]
        k_value = data[2]
        delta = data[3]

        # decompose the address of the initial Cronus object
        filename = address.split("/")[-1]
        components = filename.split(".")
        uuid = components[0]
        job_id = components[2]

        # create the hovertext template
        template = "<br>UUID: {}<br>Job_ID: {}<br>K: {}<br>Delta: {}".format(
            uuid,  # the TDigest dataset's unique ID
            job_id,  # the ID of the job that created the TDigest dataset
            k_value,
            delta,
        )

        # define the dictionary containing the histogram plotting information
        trace_data = {
            "name": name,
            "mode": "lines",
            "x": x_data,
            "y": y_data,
            "visible": True,
            "text": template,  # include text visible when hovering over a data point
            "marker": {"color": "black"},  # set the colour of the lines of data
        }

        # enclose the histogram plotting information in another dictionary with the
        # row and column subplot positioning coordinates as well as the plot type
        trace = {"data": trace_data, "row": 1, "col": 1, "plot_type": "Scatter"}

        return trace

    @staticmethod
    def _get_digest_map(tdigest=None):
        """
        A method to generate a TDigest map from an input Cronus object.
        The Cronus object contains the location of a TDigest dataset.
        This location is used to load the dataset into an empty TDigestBook object.
        The book is then decomposed into its centroid datasets,
        which are each added to a TDigest object.
        Each populated TDigest object is then used to populate a TDigest map.
        The map and the location of the TDigest dataset are both returned in a tuple.

        Parameters
        --------------------
        `tdigest`: `cronus_pb2.CronusObject`
            A Cronus protobuf object containing information on a TDigest
            dataset, including its location.

        Returns
        --------------------
        `output`: `tuple`
            A tuple of a dictionary of TDigest datasets, each containing centroid data,
            and the location of the Cronus object initially containing the TDigest data.
        """

        # create a book of the inputted tdigest data using its address
        url_data = urllib.parse.urlparse(tdigest.address)
        location = urllib.parse.unquote(url_data.path)
        book = TDigestBook.load(location)

        # define an empty TDigest map to which, centroids will be added
        digest_map = {}

        for key in book:
            # get the dataset name
            dataset_name = key[0]

            # define an empty TDigest object and add the centroids to it
            digest = TDigest()
            digest.update_centroids_from_list(book[dataset_name].centroids_to_list())

            # add the TDigest to the map, identifiable by the TDigest's name
            digest_map[dataset_name] = digest

        # define the output parameters
        output = digest_map, location

        return output

    @staticmethod
    def _validate(tdigests=None):
        """
        Method to validate the input `tdigests` parameter.
        It is expected that `tdigests` is a Protobuf Composite Container
        filled with Cronus objects.
        If the input parameter is not a container or contains no Cronus objects,
        an empty list is returned. Otherwise, a list
        of the container's Cronus objects is returned.
        If the output list does not contain any
        Cronus objects, no data can be located and no TDigests will be available.

        Parameters
        --------------------
        `tdigests`: `google.protobuf.pyext._message.RepeatedCompositeContainer`
            A protobuf container object containing TDigests to be analyzed.

        Returns
        --------------------
        `valid_tdigests`: `google.protobuf.pyext._message.RepeatedCompositeContainer`
            A protobuf container containing only validated TDigests.
        """

        # define a list to contain validated traces
        valid_tdigests = []

        # verify the `tdigests` parameter is not an invalid type or empty
        if not isinstance(
            tdigests, google.protobuf.pyext._message.RepeatedCompositeContainer
        ):
            logging.error("Invalid TDigest Container Type: '%s'.", type(tdigests))
            logging.info(
                "The container holding TDigest data was expected to be a "
                "`google.protobuf.pyext._message.repeatedCompositeContainer. "
                "Therefore, there is no TDigest data available to plot."
            )
            tdigests = []

        # verify each TDigest in the container is of the proper type
        for tdigest in tdigests:
            if not isinstance(tdigest, cronus_pb2.CronusObject):
                logging.error("Invalid TDigest Type: `%s`.", type(tdigest))
                logging.info(
                    "A TDigest object from the input container is of an invalid type. "
                    "Data from this object is not available for plotting."
                )
            else:
                valid_tdigests.append(tdigest)

        # if no valid TDigests remain, no TDigest data can be plotted
        if not valid_tdigests:
            valid_tdigests = None

            # if the valid histograms was an empty list, log why no data was available
            if isinstance(valid_tdigests, list):
                logging.error("No Valid TDigest Data Available.")
                logging.info(
                    "Each TDigest from the input container was found to be invalid. "
                    "Therefore, there is no TDigest data available for plotting."
                )

        return valid_tdigests

    def get_centroids(self, digest_map=None, name=""):
        """
        Class method for extracting centroid data from a TDigest map.
        The inputted `digest_map` is decomposed into its individual TDigest objects,
        which are passed to the `_calculate_cdf` method to produce
        a CDF 2-Dimensional dataset.
        This dataset has two properties added to it,
        the Tdigest's K-value and Delta value.
        All four elements are added to a numpy.ndarray and appended to a list.
        Once populated with an array from each TDigest, the list is returned.

        Parameters
        --------------------
        `digest_map`: `dict`
            A dictionary of TDigest datasets each containing centroid data.

        Returns
        --------------------
        `digest_data`: `list`
            A list containing arrays of two dimensions of centroid data and
            two additional descriptive statistics of the centroid data.
        """

        # keep a list containing all the arrays of centroid and related data
        digest_data = []

        # proceed only if there are still traces to analyze
        logging.info("Creating TDigest Data Using %s...", CDF_ANALYSIS_METHOD.lower())

        # iterate through each map in the TDigest and calculate each CDF
        for map_name, data in digest_map.items():
            if map_name.lower() == name:
                x_data, y_data = self._calculate_cdf(
                    tdigest=data, method=CDF_ANALYSIS_METHOD
                )
                if all([x_data is not None, y_data is not None]):
                    # grab the descriptive statistics of the tdigest data
                    k_value = data.K
                    delta = data.delta

                    # append the CDF data, the k-value and the delta value to an array
                    centroid_array = numpy.array([x_data, y_data, k_value, delta])
                    digest_data.append(centroid_array)

        return digest_data

    def generate_traces(self):
        """
        Generate a list of lists containing dictionaries of plotting instructions.
        The plotting instructions include CDF data to be plotted,
        colour schemes, and the position
        in the subplot. Only requested TDigests are included in the list of lists.
        TDigests are requested by name through the REQ_TDIGEST_TRACE_NAMES list.
        Only TDigests whose name is present in this list will have a plotting dictionary
        created containing its data. Each dictionary in a sub-list contains plotting
        information from the same TDigest object.
        Therefore, if the input container contains only one TDigest object,
        the output of this method will be a list with one element,
        a list of plotting dictionaries produced using
        the requested histograms from the single TDigest object.
        If the list of requested TDigests is invalid, all available TDigests are used.

        Returns
        --------------------
        `all_dicts`: `list`
            A list of lists of traces containing centroids and plotting data.
        """

        # get the list of valid TDigest traces names, validate it is a list
        if isinstance(REQ_TDIGEST_TRACE_NAMES, list):
            req_tdigest_names = REQ_TDIGEST_TRACE_NAMES
        elif isinstance(REQ_TDIGEST_TRACE_NAMES, str):
            req_tdigest_names = [REQ_TDIGEST_TRACE_NAMES]
        else:
            logging.error(
                "Inavlid TDigest Trace Names Variable Type: '%s'.",
                type(REQ_TDIGEST_TRACE_NAMES),
            )
            logging.info(
                "Valid TDigest trace names was expected to be a list"
                "Every available TDigest trace will be created."
            )
            req_tdigest_names = ["all"]

        # extract the tdigests data, validate it is of the proper type
        tdigests = self._validate(tdigests=self.tdigests)

        # define an empty list to contain the dictionary for each TDigest file
        all_tdigests = []

        if tdigests:
            for iteration, tdigest in enumerate(tdigests):
                logging.info("Creating TDigest Trace %s...", int(iteration) + 1)

                # get the digest map from the current tdigest
                digest_map, location = self._get_digest_map(tdigest=tdigest)

                # extract all maps and all the names of the maps from the input TDigest
                all_names = [str(name).lower() for name in digest_map]

                if not all_names:
                    logging.error("No Data in TDigest Map.")
                    logging.info(
                        "No data of any kind was found in the TDigest map. "
                        "Therefore, no TDigest traces could be created."
                    )

                # validate all the requested traces
                req_names = [str(name).lower() for name in req_tdigest_names]
                if any(["all" in req_names, "" in req_names]):
                    req_names = all_names

                for name in req_names:
                    # check for trace names that have been requested,
                    # but cannot be found in the TDigest map
                    if all([name not in all_names, name != "all"]):
                        logging.error("Invalid Requested TDigest Name: '%s'.", name)
                        logging.info(
                            "No TDigests named as above could be found. "
                            "Therefore, the requested data is not available."
                        )
                        req_names.remove(name)

                for name, data in digest_map.items():
                    # check that the trace contains data
                    if data.n <= 0.0:
                        if name.lower() in req_names:
                            req_names.remove(name.lower())

                tdigest_list = []
                for name in req_names:
                    # obtain the list of centroid arrays from the tdigest object
                    digest_data = self.get_centroids(digest_map=digest_map, name=name)

                    for centroid_array in digest_data:
                        # create a dictionary of CDF plotting information
                        centroid_dict = self._create_dict(
                            data=centroid_array, name=name, address=location
                        )
                        tdigest_list.append(centroid_dict)

                all_tdigests.append(tdigest_list)

        return all_tdigests


class MergeTraces:
    """
    Class to combine similarly named traces and modify various trace properties.
    The input traces are a list of lists of dictionaries containing plotting
    instructions and are organized based on their original object.
    Every dictionary in a sub-list is from the same `HistogramCollection` or `TDigest`.
    For plotting, histograms and TDigests will be organized by name, such that
    a subplot displays the same data, but from different objects.
    To do this, each sub-list must contain plotting dictionaries with the same name.
    Several methods in this class perform this substitution, while modifying
    the colour scheme and subplot coordinates accordingly.

    Parameters
    --------------------
    `traces`: `list`
        A list of lists containing traces from various protobufs.
    `max_cols`: `int`
        The maximum number of columns in the intended, final subplots.
    """

    def __init__(self, traces=None, max_cols=0):
        """
        Constructor class object to pass arguments to other methods.
        """

        self.traces = traces
        self.max_cols = max_cols

    @staticmethod
    def _validate(traces=None, max_cols=0):
        """
        A method to validate the input traces, their contents
        and the maximal number of subplot columns.
        The `traces` parameter was expected to be a list
        containing lists of plotting dictionaries.
        If this is not the case, the dictionaries cannot be accessed and properly
        re-organized; rather an empty list is returned instead.
        For the maximum number of subplots, the expected parameter type is an integer.
        If the parameter is not an integer or float,
        a default maximum value of 1 is used instead.

        Parameters
        --------------------
        `traces`: `list`
            A list of lists containing traces from various protobufs.
        `max_cols`: `int`
            The maximum number of columns in the intended, final subplot.

        Returns
        --------------------
        `output`: `tuple`
            Validated traces, as a list,
            and the validated number of columns as an integer.
        """

        # validate the traces parameter is a list
        if not isinstance(traces, list):
            logging.error("Invalid Traces Parameter Type: '%s'.", type(traces))
            logging.info(
                "The inputted traces were expected to be a list."
                "Therefore, the trace data cannot be accessed."
            )
            traces = []

        # validate each item in the `traces` list is, itself, a list
        valid_traces = []
        for item in traces:
            if not isinstance(item, list):
                logging.error("Invalid Trace Group Type: '%s'.", type(item))
                logging.info(
                    "Trace groups from the initial list of traces were expected to be "
                    "lists as well. Therefore, the trace group could not be accessed."
                )
            else:
                valid_traces.append(item)

        # check if any traces remain after the previous tests
        if not valid_traces:
            logging.error("No Valid Data Remaining.")
            logging.info(
                "No initial traces were found to be valid. "
                "Therefore, there is no data to merge."
            )
            valid_traces = None

        # verify the number of maximal columns is a number (integer or float)
        if isinstance(max_cols, int):
            valid_max_cols = max_cols
        elif isinstance(max_cols, float):
            valid_max_cols = int(max_cols)
        else:
            logging.error(
                "Invalid Maximum Subplot Columns Variable Type: '%s'.", type(max_cols)
            )
            logging.warning(
                "The maximum number of in one subplot row was expected to be "
                "an integer. Instead, the maximum value is defaulted to 1."
            )
            valid_max_cols = 1

        # define the output variables
        output = valid_traces, valid_max_cols

        return output

    @staticmethod
    def combine(traces=None, names=None):
        """
        A method to combine traces that share the same name.
        The list of all available names, `names`, is iterated through and each
        trace found to have the current name is added to a list.
        Once every trace named as the current name is added to the list,
        the list is appended to another list.
        Once every name has been iterated through, the output list will be a list
        of sub-lists with each sub-list containing traces with the same name.

        Parameters
        --------------------
        `traces`: `list`
            A list of lists containing traces from various protobufs.
        `names`: `list`
            A list of each of the names found in `traces`.

        Returns
        --------------------
        `combined_hists`: `list`
            A list of lists with each sublist containing similarly-named traces.
        """

        # define a list ot contain lists of similarly-named traces
        combined_traces = []

        for name in names:
            # define a list to contain similarly-named traces
            same_traces = []

            # iterate through each trace name in the list of names;
            # add each trace with a matching name to a list;
            # once all similarly-named traces are found, move on to the next name
            for trace in traces:
                trace_name = trace["data"]["name"]
                if trace_name == name:
                    same_traces.append(trace)

            # ensure the list of similarly-named traces is not
            # empty before adding it to the intended output list
            if same_traces:
                combined_traces.append(same_traces)

        return combined_traces

    @staticmethod
    def modify_coord(traces=None, max_cols=0):
        """
        A method to modify the subplot coordinates of each merged trace.
        The dictionaries in each sub-list of `traces` are intended to be
        plotted in the same subplots. Therefore, they must have the same
        `row` and `col` values. Each sub-list is isolated and each dictionary in the
        sub-list has its row and column values changed accordingly.

        Parameters
        --------------------
        `traces`: `list`
            A list of lists containing traces from various protobufs.
        `max_cols`: `int`
            The maximum number of columns in the intended, final subplot.

        Returns
        --------------------
        `all_traces`: `list`
            The list of traces with the row and column coordinates properly modified.
        """

        # define default/initial values for the row and column subplot coordinates
        row = 1
        column = 1

        # define an empty list intended to contain the adjusted merged traces
        all_traces = []

        # split every merged trace object into it's interior traces
        for merged_traces in traces:
            adj_merged_traces = []
            # assign each merged trace a row and column coordinate;
            # each merged trace must have the same subplot
            # coordinates, so they overlap in the plot
            for trace in merged_traces:
                trace["row"] = row
                trace["col"] = column
                adj_merged_traces.append(trace)

            # update the row and column coordinates;
            # ensure the column coordinate does not exceed its maximum
            if column < max_cols:
                column += 1
            else:
                row += 1
                column = 1

            # add the merged traces to the intended output list
            all_traces.append(adj_merged_traces)

        return all_traces

    @staticmethod
    def modify_colours(traces=None):
        """
        A method to modify the colour scheme of the bars in the subplots.
        It is intended that data from the same original histogram or TDigest
        object be of the same colour. Each trace is organized into sub-lists
        by its original object. Therefore, creating a list of colours and
        assigning a colour to every dictionary in each sub-list will
        ensure data from the same origin is always plotted as the same colour,
        even when organized by trace name.

        Parameters
        --------------------
        `traces`: `list`
            A list of lists containing traces from various protobufs.

        Returns
        --------------------
        `all_traces`: `list`
            The list of traces with their individual colour schemes properly modified.
        """

        # get a list of the default colours from Plotly
        pallette = DEFAULT_PLOTLY_COLORS

        # define an empty list intended to contain the adjusted merged traces
        all_traces = []

        # split every merged trace object into it's interior traces
        iteration = 0
        for non_merged_traces in traces:
            adj_traces = []
            for trace in non_merged_traces:
                # if the trace group contains many traces, recycle the default colours
                if iteration == len(pallette):
                    iteration = 0

                # change the marker colour of the trace to a default colour
                trace["data"]["marker"]["color"] = pallette[iteration]
                adj_traces.append(trace)

            # add the merged traces to the intended output list
            all_traces.append(adj_traces)
            iteration += 1

        return all_traces

    def merge(self):
        """
        A method to validate, combine, and modify input traces
        based on naming similarities. Calls
        various methods from the `MergeTraces` class to appropriately
        merge traces based on their
        naming, while modifying each trace's colour scheme and subplot coordinates.

        Returns
        --------------------
        `adj_all_traces`: `list`
            The validated traces, and the validated number of columns, as an integer.
        """

        # validate the input parameters
        traces, max_cols = self._validate(traces=self.traces, max_cols=self.max_cols)

        # change the colour of each of the markers in similar plots to demarcate them
        # ensure traces from the same protobuf file are of the same colour
        if traces:
            traces = self.modify_colours(traces=traces)

        # combine all the traces in `traces` to a single list
        all_traces = []
        for trace in traces:
            for dictionary in trace:
                all_traces.append(dictionary)

        # obtain the unique names of every histogram
        all_names = []
        for trace in all_traces:
            name = trace["data"]["name"]
            if name not in all_names:
                all_names.append(name)

        # combine the traces in separate lists based on their naming similarities
        all_traces = self.combine(traces=all_traces, names=all_names)

        # modify the coordinates of similarly-named traces, so they are
        # plotted in the same location in the intended, final subplot
        all_traces = self.modify_coord(traces=all_traces, max_cols=max_cols)

        return all_traces


class BuildFigure:
    """
    Class object to generate a figure from a list of traces, each containing data
    and plotting properties. Contains various methods to split traces, add default
    properties, create figures, and update figures with new layouts.

    Parameters
    --------------------
    `traces`: `list`
        A list of traces (dictionaries) containing data and properties to be plotted.
    `figure_type`: `str`
        The type of figure being generated.
    """

    def __init__(self, traces=None, figure_type=""):
        """
        A constructor class to pass the list of all input traces of plotting
        property dictionaries to class methods.
        """

        self.traces = traces
        self.figure_type = figure_type

    @staticmethod
    def _create_bar(traces=None, template=None):
        """
        Class method to create a bar plot trace using two traces. Systematically
        replace properties of the `template` trace with properties from the user-
        defined `trace` as they appear. Only the properties from `trace` that are
        also in `template` are replaced to avoid defining unsupported properties.

        Parameters
        --------------------
        `traces`: `list`
            A list containing dictionaries of user-defined bar plot properties.
        `template`: `dict`
            A `dict` object containing every bar plot property and it's default value.

        Returns
        --------------------
        `output_traces`: `list`
            A list of dictionaries each containing every bar plot property and it's
            default value, save for the properties specified in each inputted trace.
        """

        logging.info("Creating Bar Plot...")

        output_traces = []
        for trace_dict in traces:
            # extract the data from the trace
            data = trace_dict["data"]

            # re-define the default properties of an empty bar plot in a new variable
            bar_trace = template

            # systematically replace default properties with user-defined properties
            for key, value in data.items():
                if key in template.keys():
                    bar_trace[key] = value
                else:
                    logging.warning("Invalid Bar Plot Key: '%s'.", key)
                    logging.warning(
                        "The above key was not found in the Bar plot properties. "
                        "Therefore, this property cannot be included in the plot."
                    )

            # assign the plotting properties to the bar plot trace
            trace = go.Bar(bar_trace)

            # append the trace to the list of output traces
            output_traces.append(trace)

        return output_traces

    @staticmethod
    def _create_scatter(traces=None, template=None):
        """
        Class method to create a scatter plot trace using two traces. Systematically
        replace properties of the `template` trace with properties from the user-
        defined `trace` as they appear. Only the properties from `trace` that are
        also in `template` are replaced to avoid defining unsupported properties.

        Parameters
        --------------------
        `traces`: `list`
            A list of dictionaries of user-defined scatter plot properties.
        `template`: `dict`
            A `dict` object of every scatter plot property and it's default value.

        Returns
        --------------------
        `output_traces`: `list`
            A list of dictionaries each containing every scatter plot property and it's
            default value, save for the properties specified in each inputted trace.
        """

        logging.info("Creating Scatter...")

        output_traces = []
        for trace_dict in traces:
            # extract the data from each trace
            data = trace_dict["data"]

            # re-define default properties of an empty scatter plot in a new variable
            scatter_trace = template

            # systematically replace default properties with user-defined properties
            for key, value in data.items():
                if key in template.keys():
                    scatter_trace[key] = value
                else:
                    logging.warning("Invalid Scatter Plot Key: '%s'.", key)
                    logging.warning(
                        "The above key was not found in the Scatter plot properties. "
                        "Therefore, this property cannot be included in the plot."
                    )

            # assign the properties to the scatter plot trace
            trace = go.Scatter(scatter_trace)

            # append the trace to the list of output traces
            output_traces.append(trace)

        return output_traces

    @staticmethod
    def _validate(traces=None, figure_type=""):
        """
        Validate that `traces` amd `figure_type` are each of the proper types.
        It is expected that the `traces` parameter is a list and the `figure_type`
        parameter is a string. If this is not the case, no traces can be made available
        and/or the figure type is invalid.

        Parameters
        --------------------
        `traces`: `list`
            A list of traces containing properties, as dictionaries, to be plotted.
        `figure_type`: `str`
            The type of figure being generated.

        Returns
        --------------------
        `output`: `tuple`
            A tuple of the validated traces list and figure type.
        """

        # define a list intended to contain only valid traces
        valid_traces = []

        # check that the inputted `traces` parameter is a list
        if not isinstance(traces, list):
            logging.error("Invalid Traces Parameter Type: '%s'.", type(traces))
            logging.info(
                "Each of the histogram and TDigest traces were expected to be a list "
                "of non-empty dictionaries. Therefore, the data could not be processed."
            )
            traces = []

        # verify each trace in the inputted trace list is an non-empty list
        for trace in traces:
            if all([isinstance(trace, list), len(trace) != 0]):
                valid_traces.append(trace)
            else:
                logging.error("Invalid Trace Type: '%s'.", type(trace))
                logging.info(
                    "Each trace was expected to be a list. "
                    "Therefore, this trace could not be processed."
                )

        # check that at least one trace remains valid
        if not valid_traces:
            logging.error("No Valid Data Available.")
            logging.info(
                "The trace was found to be empty or contain no valid data. "
                "Therefore, the traces could not be processed."
            )
            valid_traces = None

        if not isinstance(figure_type, str):
            logging.error("Invalid Figure Type Parameter: '%s'.", type(figure_type))
            logging.info(
                "The input figure_type parameter was expected to be a string. "
                "Therefore, plots cannot be created."
            )
            figure_type = None

        output = valid_traces, figure_type

        return output

    @staticmethod
    def update_figure(figure=None, figure_type=""):
        """
        A class method to execute all the required updates to the layout of the figure
        object. The figure is updated with a variety of layout and axis features to
        improve the usability and readability of the saved and rendered subplots.

        Parameters
        --------------------
        `figure`: `plotly.graph_objs._figure.Figure`
            The Plotly Figure object to be updated.
        `figure_type`: `str`
            The type of figure being generated.

        Returns
        --------------------
        `figure`: `plotly.graph_objs._figure.Figure`
        """

        logging.info("Updating %s Figure Layout...", figure_type.upper())

        # change the layout for all histogram plots
        if figure_type == "histogram":
            figure.update_layout(title_text="Collection of Histogram Subplots")
            figure.update_layout(barmode="stack")
            figure.update_traces(opacity=0.7)
            figure.update_xaxes(showline=True)
            figure.update_yaxes(title_text="Frequency", showline=True)

        # change the layout for all TDigest plots
        if figure_type == "tdigest":
            figure.update_layout(title_text="Overlay of TDigest CDF Plots")
            figure.update_layout(barmode="overlay")
            figure.update_traces(opacity=0.7)

        return figure

    def generate_figure(self):
        """
        Class method to split the list of inputted traces, build a subplot figure from
        each list element, and combine each subplot into one complete figure.

        Returns
        --------------------
        `figure`: `plotly.graph_objs._figure.Figure`
        """

        # obtain and validate the inputted traces list and figure type
        traces, figure_type = self._validate(
            traces=self.traces, figure_type=self.figure_type
        )

        # ensure the figure is defined, even in the event no data is passed
        figure = None

        # define an empty list to contain all the generated traces
        trace_objects = []
        trace_names = []

        # keep track of the maximum number of rows and columns to make
        # an appropriate array of subplots
        max_rows = 0
        max_cols = 0

        if traces:
            # split the list of traces to each individual trace
            for merge_traces in traces:
                # define a trace from the list of merged traces
                first_trace = merge_traces[0]

                # extract properties of the merged traces;
                # most properties are the same, save for the data
                row = first_trace["row"]  # row location in the subplot array
                column = first_trace["col"]  # column location in the subplot array
                plot_type = first_trace["plot_type"]  # type of plot being created
                name = first_trace["data"]["name"]  # name of the trace

                logging.info("Creating %s Figure from '%s' Trace...", plot_type, name)

                # check the plot type and check that it is supported
                type_supported = True

                # build appropriate traces based on the `plot_type`
                if plot_type.lower() == "scatter":
                    logging.info("Obtaining Scatter Plot Template...")
                    traces = self._create_scatter(
                        traces=merge_traces, template=SCATTER_TEMPLATE
                    )
                elif plot_type.lower() == "bar":
                    logging.info("Obtaining Bar Plot Template...")
                    traces = self._create_bar(
                        traces=merge_traces, template=BAR_TEMPLATE
                    )
                else:
                    logging.error("Unsupported Plot Type, '%s'.", plot_type)
                    logging.info(
                        "Only Bar plots (histograms) and Scatter plots (TDigest CDF) "
                        "are supported. Therefore, the trace will not be plotted."
                    )
                    type_supported = False

                if type_supported:
                    # append the trace and its coordinates, and its name to lists
                    trace_objects.append((traces, row, column))
                    trace_names.append(name)

                    # keep track of the largest row and column coordinates
                    if row > max_rows:
                        max_rows = row
                    if column > max_cols:
                        max_cols = column

            if trace_objects:
                logging.info("Combining %s Figure(s) in Subplot...", len(trace_objects))

                # define an empty subplot figure
                figure = make_subplots(
                    rows=max_rows, cols=max_cols, subplot_titles=trace_names
                )

                # add each set of valid traces to the subplot figure
                for (traces, row, column) in trace_objects:
                    for trace in traces:
                        figure.append_trace(trace=trace, row=row, col=column)

                # update the figure by modifying it's layout, title, and axes
                figure = self.update_figure(figure=figure, figure_type=figure_type)

        return figure


class PlotlyTool:
    """
    Class object containing functions to call methods from each of the previous four
    classes.

    Parameters
    --------------------
    `store`: `artemis.meta.cronus.BaseObjectStore`
        A store object possibly containing Histogram and/or TDigest data.
    `uuid`: `string`
        A string representing the unique identifier of a dataset.
    """

    def __init__(self, store=None, uuid=""):
        """
        Constructor class object to pass arguments to other methods.
        """

        self.store = store
        self.uuid = uuid

    @staticmethod
    def _check_output(output="", check=True):
        """
        Method to check if the requested output directory exists and create it, if
        necessary. If necessary, checks with the user if files and directories can be
        created and/or deleted to produce the requested directory in which all figure
        plots are to be saved.

        Parameters
        --------------------
        `output`: `str`
            The path to the directory intended to hold the outputted files.
        `check`: `boolean`
            User's permission is required to create/delete files/directories.

        Returns
        --------------------
        `proceed`: `boolean`
            Indicate if the tool can proceed to plotting histograms and TDigests.
        """

        # check the requested output location, determine if it exists
        proceed = True
        if not os.path.exists(output):
            if check:
                # if the intended output location does not exist, ask to create it
                print("")
                print("The requested output file location does not exist.")
                create = input("Create it? 'Yes' or 'No' >>> ")
                proceed = bool(create.lower() in ["y", "yes"])
                print("")
            if any([proceed, not check]):
                # attempt to create the requested directory
                try:
                    logging.info("Creating output directory...")
                    os.mkdir(output)
                except OSError:
                    logging.error("Unable to create output directory.")
                    proceed = False

        return proceed

    @staticmethod
    def _list(store=None, uuid=""):
        """
        A method to list the histograms and TDigests in the inputted store and present
        at the specified ID. The output is a tuple of two elements: a container of
        histogram collection objects and a container of TDigest datasets.

        Parameters
        --------------------
        `store`: `artemis.meta.cronus.BaseObjectStore`
            A store object possibly containing histogram nad/or TDigest data.
        `uuid`: `string`
            A string representing the unique identifier of a dataset.

        Returns
        --------------------
        `output`: `tuple`
            A tuple containing the histogram and TDigest containers from the store.
        """

        # if any inputs are invalid, no histograms or TDigests can be created
        if any([not store, not uuid]):
            histograms = None
            tdigests = None
        else:
            histograms = store.list_histograms(uuid)
            tdigests = store.list_tdigests(uuid)

        # return the histogram and TDigest datasets
        output = histograms, tdigests

        return output

    @staticmethod
    def _validate(store=None, uuid=""):
        """
        A method to validate that the inputted store object and UUID are both of the
        proper type.  It is expected that the store be a `BaseObjectStore` and the UUID
        be a string. If this is not the case, no store and/or UUID can be used due to
        incompatibility.

        Parameters
        --------------------
        `store`: `artemis.meta.cronus.BaseObjectStore`
            A store object possibly containing histogram and/or TDigest data.
        `uuid`: `string`
            A string representing the unique identifier of a dataset.

        Returns
        --------------------
        `output`: `tuple`
            A tuple containing the validated input parameters.
        """

        # validate the inputted store is of the proper type
        if not isinstance(store, BaseObjectStore):
            logging.error("Invalid Input Store Type: '%s'.", type(store))
            logging.info(
                "Input `store` parameter was expected to be a `BaseObjectStore`. "
                "Therefore, the provided store could not be used."
            )
            store = None

        # validate the inputted UUID is of the proper type
        if not isinstance(uuid, str):
            logging.error("Invalid Input UUID Type: '%s'.", type(uuid))
            logging.info(
                "The inputted `uuid` parameter was expected to be a string. "
                "Therefore, the provided UUID could not be used."
            )
            uuid = None

        # return the parameters
        output = store, uuid

        return output

    @staticmethod
    def get_figure(traces=None, output="", show=True, check=True, fig_type=""):
        """
        A method to generate Plotly Figure objects, render them, and save them as HTML
        files in the directory specified by `output`. The `generate_figure` method of
        `BuildFigure` is called to generate the plots for histograms and TDigests, while
        the `plot_save` function imported from Plotly saved and/or renders the plots.

        Parameters
        --------------------
        `traces`: `list`
            A list of traces to be converted into a Plotly Figure object.
        `output`: `str`
            The path to the directory intended to hold the outputted files.
        `check`: `boolean`
            User's permission is required to create/delete files/directories.
        `show`: `boolean`
            A boolean indicating if the plots are rendered as well as saved.
        `fig_type`: `str`
            The type of traces being provided in `traces`.
        """

        # check if the file already exists and request overwriting, if needed
        proceed = True
        if os.path.exists(output):
            if check:
                print("")
                print(
                    "A {} file already exists at the location intended for "
                    "a new, similar output file, '{}'.".format(fig_type, output)
                )
                create = input("Overwrite it? 'Yes' or 'No' >>> ")
                proceed = bool(create.lower() in ["y", "yes"])
                print("")

        if proceed:
            # define the BuidFigure class and create a figure from the group of traces
            logging.info(
                "Building Figure from %s %s Traces...", len(traces), fig_type.upper()
            )
            plot_figure = BuildFigure(
                traces=traces, figure_type=fig_type
            ).generate_figure()

            if plot_figure:
                # define an output file location and save the figure as an HTML file
                action = "Saving and Rendering" if show else "Saving"
                logging.info("%s %s Subplot Figure...", action, fig_type.upper())
                plot_save(plot_figure, filename=output, auto_open=show)

            # after saving the file, verify that it exists
            if not os.path.exists(output):
                logging.error("Could Not Save Plot.")
                logging.info(
                    "The plot could not be saved as an %s %s file located at '%s'.",
                    fig_type.upper(),
                    output.split(".")[-1].upper(),
                    output,
                )
        else:
            logging.info("%s File and Figure not created.", fig_type.upper())

    def visualize(self, output="", show=True, check=True):
        """
        A method to perform various functions by calling several classes and their
        methods to validate parameters, create traces, build plots as figure objects, as
        well as save and/or render the plots.

        Parameters
        --------------------
        `output`: `str`
            The path to the directory intended to hold the outputted files.
        `show`: `boolean`
            A boolean indicating if the plots are rendered as well as saved.
        `check`: `boolean`
            User's permission is required to create/delete files/directories.
        """

        # begin logging
        logging.info("Executing `PlotlyTool`...")

        # start a timer, and log it, to record the runtime of the full algorithm
        start = time.time()
        logging.info("PlotlyTool procedure started at %s.", time.asctime())

        # log whether user suthorization was enabled or disabled
        logging.info("User authorization %s.", "enabled" if check else "disabled")

        if self._check_output(output=output, check=check):
            # validate the input store and UUID
            store, uuid = self._validate(store=self.store, uuid=self.uuid)

            # extract the histogram and TDigest datasets from the store
            histograms, tdigests = self._list(store=store, uuid=uuid)

            # get the traces from the histogram class method
            logging.info(
                "=================================================================="
            )
            logging.info("Creating Histogram Traces...")

            # generate a list of lists of traces from the histograms in the input store
            hist_traces = ProcessHist(histograms=histograms).generate_traces()

            # merge together traces with the same name
            hist_traces = MergeTraces(
                traces=hist_traces, max_cols=MAX_HIST_SUBPLOT_COLUMNS
            ).merge()

            # if traces from the histograms are present, create the figure(s)
            if len(hist_traces) != 0:
                # define the intended output file
                save_file = "{}/histogram_plot.html".format(output)

                # generate and save/show the figure
                logging.info("Generating Histogram Figure...")
                self.get_figure(
                    traces=hist_traces,
                    output=save_file,
                    check=check,
                    show=show,
                    fig_type="histogram",
                )
            else:
                logging.error("Histogram Processing Tool Failed.")
                logging.info(
                    "The histogram processing procedure encountered an error. "
                    "As a result, no histograms could be created or plotted."
                )

            # get the traces from the tdigest class method
            logging.info(
                "=================================================================="
            )
            logging.info("Creating TDigest Traces...")

            # generate a list of traces from the TDigests in the input store
            tdigest_traces = ProcessTDigest(tdigests=tdigests).generate_traces()

            # merge together traces with the same name
            hist_traces = MergeTraces(
                traces=tdigest_traces, max_cols=MAX_TDIGEST_SUBPLOT_COLUMNS
            ).merge()

            # if traces from the tdigests are present, create the figure(s)
            if len(tdigest_traces) != 0:
                # define the intended output file
                save_file = "{}/tdigest_plot.html".format(output)

                # generate and save/show the figure
                logging.info("Generating TDigest Figure...")
                self.get_figure(
                    traces=tdigest_traces,
                    output=save_file,
                    check=check,
                    show=show,
                    fig_type="tdigest",
                )
            else:
                logging.error("TDigest Processing Tool Failed.")
                logging.info(
                    "The TDigest processing procedure encountered an error. "
                    "As a result, no TDigests could be created or CDFs plotted."
                )
            logging.info(
                "=================================================================="
            )
        else:
            logging.info("`PlotlyTool` execution aborted.")

        # record the time when the program was completed or was stopped and how long it
        # ran for
        logging.info("Execution started at %s", time.asctime())
        logging.info(
            "Time to Execute `PlotlyTool`: %s seconds.", round(time.time() - start, 2)
        )
