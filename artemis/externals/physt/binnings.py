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
"""Different binning algorithms/schemas for the histograms."""
from collections import OrderedDict
from typing import Optional, Tuple, Union

import numpy as np

from .bin_utils import (
    is_bin_subset,
    is_consecutive,
    is_rising,
    make_bin_array,
    to_numpy_bins,
    to_numpy_bins_with_mask,
)
from .util import find_subclass

# TODO: Locking and edit operations (like numpy read-only)


class BinningBase:
    """Abstract base class for binning schemas.

    Inheriting
    ----------
    - define at least one of the following properties:
    bins, numpy_bins (cached conversion exists)
    - if you modify bins, put _bins and _numpy_bins
    into proper state (None may be sufficient)
    - checking of proper bins should be done in __init__
    - if you want to support adaptive histogram, override _force_bin_existence
    - implement _update_dict to contain the binning representation
    - the constructor (and facade methods)
    must accept any kwargs (and ignores those that are not used).

    Attributes
    ----------
    adaptive_allowed : bool
        Whether is possible to update the bins dynamically
    inconsecutive_allowed : bool
        Whether it is possible to have bins with gaps

    TODO: Check the last point (does it make sense?)
    """

    def __init__(
        self, bins=None, numpy_bins=None, includes_right_edge=False, adaptive=False
    ):
        # TODO: Incorporate integrity_check?
        self._consecutive = None
        if bins is not None:
            if numpy_bins is not None:
                raise RuntimeError(
                    "Cannot specify numpy_bins " "and bins at the same time."
                )
            bins = make_bin_array(bins)
            if not is_rising(bins):
                raise RuntimeError("Bins must be in rising order.")
            # TODO: Test for consecutiveness?
        elif numpy_bins is not None:
            numpy_bins = to_numpy_bins(numpy_bins)
            if not np.all(numpy_bins[1:] > numpy_bins[:-1]):
                raise RuntimeError("Bins must be in rising order.")
            self._consecutive = True
        self._bins = bins
        self._numpy_bins = numpy_bins
        self._includes_right_edge = includes_right_edge
        if adaptive and not self.adaptive_allowed:
            raise RuntimeError(
                "Adaptivity not allowed for {0}".format(self.__class__.__name__)
            )
        if adaptive and includes_right_edge:
            raise RuntimeError(
                "Adaptivity does not work " "together with right-edge inclusion."
            )
        self._adaptive = adaptive

    def __getitem__(self, index):
        if isinstance(index, slice):
            new_binning = self.as_static()
            new_binning._bins = new_binning.bins[index]
            return new_binning
        else:
            return self.bins[index]

    @staticmethod
    def from_dict(a_dict):
        binning_type = a_dict.pop("binning_type", "StaticBinning")
        klass = find_subclass(BinningBase, binning_type)
        return klass(**a_dict)

    adaptive_allowed = False
    inconsecutive_allowed = False
    # TODO: adding allowed?

    def to_dict(self) -> OrderedDict:
        """Dictionary representation of the binning schema.

        This serves as template method, please implement _update_dict
        """
        result = OrderedDict()
        result["adaptive"] = self._adaptive
        result["binning_type"] = type(self).__name__
        self._update_dict(result)
        return result

    def _update_dict(self, a_dict):
        raise NotImplementedError(
            "Dictionary representation of {0} "
            "is not implemented.".format(type(self).__name__)
        )

    @property
    def includes_right_edge(self) -> bool:
        # TODO: Document and explain
        return self._includes_right_edge

    def is_regular(self, rtol: float = 1.0e-5, atol: float = 1.0e-8) -> bool:
        """Whether all bins have the same width.

        Parameters
        ----------
        rtol, atol : numpy tolerance parameters
        """
        return np.allclose(
            np.diff(self.bins[1] - self.bins[0]), 0.0, rtol=rtol, atol=atol
        )

    def is_consecutive(self, rtol: float = 1.0e-5, atol: float = 1.0e-8) -> bool:
        """Whether all bins are in a growing order.

        Parameters
        ----------
        rtol, atol : numpy tolerance parameters
        """
        if self.inconsecutive_allowed:
            if self._consecutive is None:
                if self._numpy_bins is not None:
                    self._consecutive = True
                self._consecutive = is_consecutive(self.bins, rtol, atol)
            return self._consecutive
        else:
            return True

    def is_adaptive(self) -> bool:
        """
        Whether the binning can be adapted
        to include values not currently spanned.
        """
        return self._adaptive

    def force_bin_existence(self, values):
        """Change schema so that there is a bin for value.

        It is necessary to implement the _force_bin_existence template method.

        Parameters
        ----------
        values: np.ndarray
            All values we want bins for.

        Returns
        -------
        bin_map: Iterable[tuple] or None or int
            None => There was no change in bins
            int => The bins are only shifted (allows mass assignment)
            Otherwise => the iterable contains tuples
                (old bin index, new bin index)
                new bin index can occur multiple times,
                which corresponds to bin merging
        """
        # TODO: Rename to something less evil
        if not self.is_adaptive():
            raise RuntimeError("Histogram is not adaptive")
        else:
            return self._force_bin_existence(values)

    def _force_bin_existence(self, values):
        # TODO: in-place
        raise NotImplementedError()

    def adapt(self, other: "BinningBase"):
        """Adapt this binning so that it contains all bins of another binning.

        Parameters
        ----------
        other: BinningBase
        """
        # TODO: in-place arg
        if np.array_equal(self.bins, other.bins):
            return None, None
        elif not self.is_adaptive():
            raise RuntimeError("Cannot adapt non-adaptive binning.")
        else:
            return self._adapt(other)

    def set_adaptive(self, value: bool = True) -> None:
        """Set/unset the adaptive property of the binning.

        This is available only for some of the binning types.
        """
        if value and not self.adaptive_allowed:
            raise RuntimeError("Cannot change binning to adaptive.")
        self._adaptive = value

    def _adapt(self, other):
        raise RuntimeError("Cannot adapt binning.")

    @property
    def bins(self) -> np.ndarray:
        """Bins in the wider format (as edge pairs)

        Returns
        -------
        bins: np.ndarray
            shape=(bin_count, 2)
        """
        if self._bins is None:
            self._bins = make_bin_array(self.numpy_bins)
        return self._bins

    @property
    def bin_count(self) -> int:
        """The total number of bins."""
        return self.bins.shape[0]

    @property
    def numpy_bins(self) -> np.ndarray:
        """Bins in the numpy format

        This might not be available for inconsecutive binnings.

        Returns
        -------
        edges: np.ndarray
            shape=(bin_count+1,)
        """
        if self._numpy_bins is None:
            self._numpy_bins = to_numpy_bins(self.bins)
        return self._numpy_bins

    @property
    def numpy_bins_with_mask(self) -> Tuple[np.ndarray, np.ndarray]:
        """Bins in the numpy format, including the gaps in inconsecutive binnings.

        Returns
        -------
        edges, mask: np.ndarray

        See Also
        --------
        bin_utils.to_numpy_bins_with_mask
        """
        bwm = to_numpy_bins_with_mask(self.bins)
        if not self.includes_right_edge:
            bwm[0].append(np.inf)
        return bwm

    @property
    def first_edge(self) -> float:
        """The left edge of the first bin."""
        if self._numpy_bins is None:
            return self._numpy_bins[0]
        else:
            return self.bins[0][0]

    @property
    def last_edge(self) -> float:
        """The right edge of the last bin."""
        if self._numpy_bins is None:
            return self._numpy_bins[-1]
        else:
            return self.bins[-1][1]

    def as_static(self, copy: bool = True) -> "StaticBinning":
        """Convert binning to a static form.

        Parameters
        ----------
        copy: bool
            Ensure that we receive another object

        Returns
        -------
        StaticBinning
            A new static binning with a copy of bins.
        """
        return StaticBinning(
            bins=self.bins.copy(), includes_right_edge=self.includes_right_edge
        )

    def as_fixed_width(self, copy=True):
        """Convert binning to recipe with fixed width (if possible.)

        Parameters
        ----------
        copy: bool
            Ensure that we receive another object

        Returns
        -------
        FixedWidthBinning
        """
        if self.bin_count == 0:
            raise RuntimeError("Cannot guess binning width with zero bins")
        elif self.bin_count == 1 or self.is_consecutive() and self.is_regular():
            return FixedWidthBinning(
                min=self.bins[0][0],
                bin_count=self.bin_count,
                bin_width=self.bins[1] - self.bins[0],
            )
        else:
            raise RuntimeError(
                "Cannot create fixed-width binning " "from differing bin widths."
            )

    def copy(self) -> "BinningBase":
        """An identical, independent copy."""
        raise NotImplementedError()

    def apply_bin_map(self, bin_map) -> "BinningBase":
        """

        Parameters
        ----------
        bin_map: Iterator(tuple)
            The bins must be in ascending order
        """
        length = max(item[1] for item in bin_map) + 1
        bins = np.empty((length, 2), dtype=float)
        bins[:] = np.nan
        for old, new in bin_map:
            if np.isnan(bins[new, 0]):
                bins[new, :] = self.bins[old, :]
            else:
                if bins[new, 1] != self.bins[old, 0]:
                    raise RuntimeError("Merging non-consecutive bins")
                bins[new, 1] = self.bins[old, 1]
        if np.any(np.isnan(bins)):
            raise RuntimeError("New binning is not complete.")
        includes_right_edge = (
            self.includes_right_edge and bins[-1, 1] == self.bins[-1, 1]
        )
        binning = StaticBinning(bins, includes_right_edge=includes_right_edge)
        return binning

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.numpy_bins))


class StaticBinning(BinningBase):
    inconsecutive_allowed = True

    def __init__(self, bins, includes_right_edge=True, **kwargs):
        super(StaticBinning, self).__init__(
            bins=bins, includes_right_edge=includes_right_edge
        )

    def as_static(self, copy: bool = True) -> "StaticBinning":
        """Convert binning to a static form.

        Returns
        -------
        StaticBinning
            A new static binning with a copy of bins.

        Parameters
        ----------
        copy : if True, returns itself (already satisfying conditions).
        """
        if copy:
            return StaticBinning(
                bins=self.bins.copy(), includes_right_edge=self.includes_right_edge
            )
        else:
            return self

    def copy(self):
        return self.as_static(True)

    def __getitem__(self, item):
        copy = self.copy()
        copy._bins = self._bins[item]
        # TODO: check for the right_edge??
        return copy

    def _update_dict(self, a_dict):
        a_dict["bins"] = self.bins.tolist()

    def _adapt(self, other):
        if is_bin_subset(other.bins, self.bins):
            indices = np.searchsorted(other.bins[:, 0], self.bins[:, 0])
            return None, list(enumerate(indices))

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.bins))


class NumpyBinning(BinningBase):
    """Binning schema working as numpy.histogram.
    """

    def __init__(self, numpy_bins, includes_right_edge=True, **kwargs):
        if not is_rising(numpy_bins):
            raise RuntimeError("Bins not in rising order.")
        super(NumpyBinning, self).__init__(
            numpy_bins=numpy_bins, includes_right_edge=includes_right_edge, **kwargs
        )

    @property
    def numpy_bins(self):
        return self._numpy_bins

    def copy(self) -> "NumpyBinning":
        return NumpyBinning(
            numpy_bins=self.numpy_bins, includes_right_edge=self.includes_right_edge
        )

    def _update_dict(self, a_dict: dict) -> None:
        a_dict["numpy_bins"] = self.numpy_bins.tolist()


class FixedWidthBinning(BinningBase):
    """Binning schema with predefined bin width."""

    adaptive_allowed = True

    def __init__(
        self,
        bin_width,
        bin_count=0,
        bin_times_min=None,
        min=None,
        includes_right_edge=False,
        adaptive=False,
        bin_shift=None,
        align=True,
        **kwargs
    ):
        super(FixedWidthBinning, self).__init__(
            adaptive=adaptive, includes_right_edge=includes_right_edge
        )

        # TODO: Check edge cases for min/shift/align
        if bin_width <= 0:
            raise RuntimeError("Bin width must be > 0.")
        if bin_count < 0:
            raise RuntimeError("Bin count must be >= 0.")
        if (bin_times_min is not None or bin_shift is not None) and (min is not None):
            raise RuntimeError("Cannot specify both min " "and (times_min or shift)")
        self._bin_width = float(bin_width)
        self._align = align
        self._bin_count = int(bin_count)
        if min is not None:
            self._times_min = int(np.floor(min / self.bin_width))
            self._shift = min - self._times_min * self.bin_width
        else:
            self._times_min = bin_times_min
            self._shift = bin_shift or 0.0
        self._bins = None
        self._numpy_bins = None

    def __repr__(self):
        result = "{0}(bin_width={1}, bin_count={2}, min={3}".format(
            self.__class__.__name__, self.bin_width, self.bin_count, self.first_edge
        )
        if self.is_adaptive():
            result += ", adaptive=True"
        return result + ")"

    def is_regular(self, *args, **kwargs) -> bool:
        return True

    def _force_bin_existence_single(self, value, includes_right_edge=None):
        if includes_right_edge is None:
            includes_right_edge = self.includes_right_edge

        if self._bin_count == 0:
            self._times_min = int(np.floor((value - self._shift) / self.bin_width))
            if not self._align:
                self._shift = value - self._times_min * self.bin_width
            self._bin_count = 1
            self._bins = None
            self._numpy_bins = None
            return ()
        else:
            add_left = add_right = 0
            if value < self.numpy_bins[0]:
                add_left = int(np.ceil((self.numpy_bins[0] - value) / self.bin_width))
                self._times_min -= add_left
                self._bin_count += add_left
            elif value >= self.numpy_bins[-1]:
                add_right = (value - self.numpy_bins[-1]) / self.bin_width
                add_right = int(np.ceil(add_right))
                self._bin_count += add_right
                if self.last_edge == value and not includes_right_edge:
                    add_right += 1
                    self._bin_count += 1
            if add_left or add_right:
                self._bins = None
                self._numpy_bins = None
                return add_left
            else:
                return None

    def _force_bin_existence(self, values, includes_right_edge=None):
        if np.isscalar(values):
            return self._force_bin_existence_single(
                values, includes_right_edge=includes_right_edge
            )
        else:
            min, max = np.min(values), np.max(values)
            result = self._force_bin_existence_single(min)
            result2 = self._force_bin_existence_single(
                max, includes_right_edge=includes_right_edge
            )
            if result is None:
                return result2
            else:
                return result

    @property
    def first_edge(self) -> float:
        return self._times_min * self._bin_width + self._shift

    @property
    def last_edge(self) -> float:
        return (self._times_min + self._bin_count) * self._bin_width + self._shift

    @property
    def numpy_bins(self):
        if self._numpy_bins is None:
            self._bins = None
            if self._bin_count == 0:
                return np.zeros((0, 2), dtype=float)
            self._numpy_bins = (
                self._times_min + np.arange(self._bin_count + 1, dtype=int)
            ) * self._bin_width + self._shift
        return self._numpy_bins

    @property
    def bin_count(self):
        return self._bin_count

    def copy(self):
        return FixedWidthBinning(
            bin_width=self._bin_width,
            bin_count=self._bin_count,
            align=self._align,  # Not necessary
            bin_times_min=self._times_min,
            bin_shift=self._shift,
            includes_right_edge=self.includes_right_edge,
            adaptive=self._adaptive,
        )

    @property
    def bin_width(self):
        return self._bin_width

    def _force_new_min_max(self, new_min, new_max):
        bin_map = None
        add_right = add_left = 0
        if new_min < self._times_min:
            add_left = self._times_min - new_min
        if new_max - self._times_min > self._bin_count:
            add_right = new_max - self._times_min - self._bin_count
        if add_left or add_right:
            bin_map = ((i, i + add_left) for i in range(self._bin_count))
            self._set_min_and_count(
                self._times_min - add_left, self._bin_count + add_left + add_right
            )
        return bin_map

    def _set_min_and_count(self, times_min, bin_count):
        self._bin_count = bin_count
        self._times_min = times_min
        self._bins = None
        self._numpy_bins = None

    def _adapt(self, other: BinningBase):
        """

        Returns
        -------
        bin_map1: Iterable[tuple] or None
        bin_map2: Iterable[tuple] or None
        """
        other = other.as_fixed_width()
        if self.bin_width != other.bin_width:
            raise RuntimeError(
                "Cannot adapt fixed-width " "histograms with different widths"
            )
        if self._shift != other._shift:
            raise RuntimeError(
                "Cannot adapt shifted fixed-width "
                "histograms: {0} vs {1}".format(self._shift, other._shift)
            )
        # Following operations modify schemas
        other = other.copy()
        if other.bin_count == 0:
            return None, ()
        if self.bin_count == 0:
            self._set_min_and_count(other._times_min, other.bin_count)
            return (), None
        new_min = min(self._times_min, other._times_min)
        new_max = max(
            self._times_min + self._bin_count, other._times_min + other._bin_count
        )

        bin_map1 = self._force_new_min_max(new_min, new_max)
        bin_map2 = other._force_new_min_max(new_min, new_max)
        return bin_map1, bin_map2

    def as_fixed_width(self, copy=True):
        if copy:
            return self.copy()
        else:
            return self

    def _update_dict(self, a_dict):
        # TODO: Fix to be instantiable from JSON
        a_dict["bin_count"] = self.bin_count
        a_dict["bin_width"] = self.bin_width
        a_dict["bin_shift"] = self._shift
        a_dict["bin_times_min"] = self._times_min


class ExponentialBinning(BinningBase):
    """Binning schema with exponentially distributed bins."""

    adaptive_allowed = False

    # TODO: Implement adaptivity

    def __init__(
        self,
        log_min,
        log_width,
        bin_count,
        includes_right_edge=True,
        adaptive=False,
        **kwargs
    ):
        super(ExponentialBinning, self).__init__(
            includes_right_edge=includes_right_edge, adaptive=adaptive
        )
        self._log_min = log_min
        self._log_width = log_width
        self._bin_count = bin_count

    def is_regular(self, *args, **kwargs) -> bool:
        return False

    @property
    def numpy_bins(self):
        if self._bin_count == 0:
            return np.ndarray((0,), dtype=float)
        if self._numpy_bins is None:
            log_bins = self._log_min + np.arange(self._bin_count + 1) * self._log_width
            self._numpy_bins = 10.0 ** log_bins
        return self._numpy_bins

    def copy(self) -> "ExponentialBinning":
        return ExponentialBinning(
            self._log_min, self._log_width, self._bin_count, self.includes_right_edge
        )

    def _update_dict(self, a_dict):
        a_dict["log_min"] = self._log_min
        a_dict["log_width"] = self._log_width
        a_dict["bin_count"] = self._bin_count


def numpy_binning(data, bins=10, range=None, *args, **kwargs) -> NumpyBinning:
    """Construct binning schema compatible with numpy.histogram

    Parameters
    ----------
    data: array_like, optional
        This is optional if both bins and range are set
    bins: int or array_like
    range: Optional[tuple]
        (min, max)
    includes_right_edge: Optional[bool]
        default: True

    See Also
    --------
    numpy.histogram
    """
    if isinstance(bins, int):
        if range:
            bins = np.linspace(range[0], range[1], bins + 1)
        else:
            start = data.min()
            stop = data.max()
            bins = np.linspace(start, stop, bins + 1)
    elif np.iterable(bins):
        bins = np.asarray(bins)
    else:
        # Some numpy edge case
        _, bins = np.histogram(data, bins, **kwargs)
    return NumpyBinning(bins)


def human_binning(
    data=None, bin_count: Optional[int] = None, *, range=None, **kwargs
) -> FixedWidthBinning:
    """
    Construct fixed-width binning schema with
    bins automatically optimized to human-friendly widths.

    Typical widths are: 1.0, 25,0, 0.02, 500, 2.5e-7, ...

    Parameters
    ----------
    bin_count: Number of bins
    range: Optional[tuple]
        (min, max)
    """
    subscales = np.array([0.5, 1, 2, 2.5, 5, 10])

    # TODO: remove colliding kwargs
    if data is None and range is None:
        raise RuntimeError("Cannot guess optimum bin width without data.")
    if bin_count is None:
        bin_count = ideal_bin_count(data)
    min_ = range[0] if range else data.min()
    max_ = range[1] if range else data.max()
    bw = (max_ - min_) / bin_count

    power = np.floor(np.log10(bw)).astype(int)
    best_index = np.argmin(np.abs(np.log(subscales * (10.0 ** power) / bw)))
    bin_width = (10.0 ** power) * subscales[best_index]
    return fixed_width_binning(bin_width=bin_width, data=data, range=range, **kwargs)


def quantile_binning(
    data=None, bins=10, *, qrange=(0.0, 1.0), **kwargs
) -> StaticBinning:
    """Binning schema based on quantile ranges.

    This binning finds equally spaced quantiles. This should lead to
    all bins having roughly the same frequencies.

    Note: weights are not (yet) take into account for calculating
    quantiles.

    Parameters
    ----------
    bins: sequence or Optional[int]
        Number of bins
    qrange: Optional[tuple]
        Two floats as minimum and maximum quantile (default: 0.0, 1.0)

    Returns
    -------
    StaticBinning
    """

    if np.isscalar(bins):
        bins = np.linspace(qrange[0] * 100, qrange[1] * 100, bins + 1)
    bins = np.percentile(data, bins)
    return static_binning(bins=make_bin_array(bins), includes_right_edge=True)


def static_binning(data=None, bins=None, **kwargs) -> StaticBinning:
    """Construct static binning with whatever bins."""
    return StaticBinning(bins=make_bin_array(bins), **kwargs)


def integer_binning(data=None, **kwargs) -> FixedWidthBinning:
    """Construct fixed-width binning schema with bins centered around integers.

    Parameters
    ----------
    range: Optional[Tuple[int]]
        min (included) and max integer (excluded) bin
    bin_width: Optional[int]
        group "bin_width" integers into one bin (not recommended)
    """
    if "range" in kwargs:
        kwargs["range"] = tuple(r - 0.5 for r in kwargs["range"])
    return fixed_width_binning(
        data=data,
        bin_width=kwargs.pop("bin_width", 1),
        align=True,
        bin_shift=0.5,
        **kwargs
    )


def fixed_width_binning(
    data=None,
    bin_width: Union[float, int] = 1,
    *,
    range=None,
    includes_right_edge=False,
    **kwargs
) -> FixedWidthBinning:
    """Construct fixed-width binning schema.

    Parameters
    ----------
    bin_width: float
    range: Optional[tuple]
        (min, max)
    align: Optional[float]
        Must be multiple of bin_width
    """
    result = FixedWidthBinning(
        bin_width=bin_width, includes_right_edge=includes_right_edge, **kwargs
    )
    if range:
        result._force_bin_existence(range[0])
        result._force_bin_existence(range[1], includes_right_edge=True)
        if not kwargs.get("adaptive"):
            return result  # Otherwise we want to adapt to data
    if data is not None and data.shape[0]:
        # print("Jo, tady")
        result._force_bin_existence(
            [np.min(data), np.max(data)], includes_right_edge=includes_right_edge
        )
    return result


def exponential_binning(
    data=None, bin_count: Optional[int] = None, *, range=None, **kwargs
) -> ExponentialBinning:
    """Construct exponential binning schema.

    Parameters
    ----------
    bin_count: Optional[int]
        Number of bins
    range: Optional[tuple]
        (min, max)

    See also
    --------
    numpy.logspace - note that our range semantics is different
    """
    if bin_count is None:
        bin_count = ideal_bin_count(data)

    if range:
        range = (np.log10(range[0]), np.log10(range[1]))
    else:
        range = (np.log10(data.min()), np.log10(data.max()))
    log_width = (range[1] - range[0]) / bin_count
    return ExponentialBinning(
        log_min=range[0], log_width=log_width, bin_count=bin_count, **kwargs
    )


def calculate_bins(array, _=None, *args, **kwargs) -> BinningBase:
    """Find optimal binning from arguments.

    Parameters
    ----------
    array: arraylike
        Data from which the bins should be decided
        (sometimes used, sometimes not)
    _: int or str or Callable or arraylike or Iterable or BinningBase
        To-be-guessed parameter that specifies
        what kind of binning should be done
    check_nan: bool
        Check for the presence of nan's in array? Default: True
    range: tuple
        Limit values to a range. Some of the binning methods also
        (subsequently) use this parameter for the bin shape.

    Returns
    -------
    BinningBase
        A two-dimensional array with pairs of bin edges
        (not necessarily consecutive).

    """
    if array is not None:
        if kwargs.pop("check_nan", True):
            if np.any(np.isnan(array)):
                raise RuntimeError("Cannot calculate bins " "in presence of NaN's.")
        # TODO: re-consider the usage of this parameter
        if kwargs.get("range", None):
            array = array[(array >= kwargs["range"][0]) & (array <= kwargs["range"][1])]
    if _ is None:
        # kwargs.pop("bins", ideal_bin_count(data=array)) - same as numpy
        bin_count = 10
        binning = numpy_binning(array, bin_count, *args, **kwargs)
    elif isinstance(_, BinningBase):
        binning = _
    elif isinstance(_, int):
        binning = numpy_binning(array, _, *args, **kwargs)
    elif isinstance(_, str):
        # What about the ranges???
        if _ in bincount_methods:
            bin_count = ideal_bin_count(array, method=_)
            binning = numpy_binning(array, bin_count, *args, **kwargs)
        elif _ in binning_methods:
            method = binning_methods[_]
            binning = method(array, *args, **kwargs)
        else:
            raise RuntimeError("No binning method {0} available.".format(_))
    elif callable(_):
        binning = _(array, *args, **kwargs)
    elif np.iterable(_):
        binning = static_binning(array, _, *args, **kwargs)
    else:
        raise RuntimeError("Binning {0} not understood.".format(_))
    return binning


def calculate_bins_nd(array, bins=None, *args, **kwargs):
    """Find optimal binning from arguments (n-dimensional variant)

    Usage similar to `calculate_bins`.

    Returns
    -------
    List[BinningBase]
    """
    if kwargs.pop("check_nan", True):
        if np.any(np.isnan(array)):
            raise RuntimeError("Cannot calculate bins in presence of NaN's.")

    if array is not None:
        _, dim = array.shape

    # Prepare bins
    if isinstance(bins, (list, tuple)):
        if len(bins) != dim:
            raise RuntimeError(
                "List of bins not understood, "
                "expected {0} items, got {1}.".format(dim, len(bins))
            )
    else:
        bins = [bins] * dim

    # Prepare arguments
    args = list(args)
    range_ = kwargs.pop("range", None)
    if range_:
        if len(range_) == 2 and all(np.isscalar(i) for i in range_):
            range_ = dim * [range_]
        elif len(range_) != dim:
            raise RuntimeError("Wrong dimensionality of range")
    for i in range(len(args)):
        if isinstance(args[i], (list, tuple)):
            if len(args[i]) != dim:
                raise RuntimeError("Argument not understood.")
        else:
            args[i] = dim * [args[i]]
    for key in list(kwargs.keys()):
        if isinstance(kwargs[key], (list, tuple)):
            if len(kwargs[key]) != dim:
                raise RuntimeError("Argument not understood.")
        else:
            kwargs[key] = dim * [kwargs[key]]

    if range_:
        kwargs["range"] = range_

    bins = [
        calculate_bins(
            array[:, i],
            bins[i],
            *(arg[i] for arg in args if arg[i] is not None),
            **{k: kwarg[i] for k, kwarg in kwargs.items() if kwarg[i] is not None}
        )
        for i in range(dim)
    ]
    return bins


# TODO: Rename
binning_methods = {
    "numpy": numpy_binning,
    "exponential": exponential_binning,
    "quantile": quantile_binning,
    "fixed_width": fixed_width_binning,
    "integer": integer_binning,
    "human": human_binning,
}


try:
    # If possible, import astropy's binning methods
    # See: http://docs.astropy.org/en/stable/visualization/histogram.html
    # Just check
    from astropy.stats.histogram import histogram as _astropy_histogram  # noqa: F401
    import warnings

    warnings.filterwarnings("ignore", module="astropy\\..*")

    def bayesian_blocks_binning(data, range=None, **kwargs) -> NumpyBinning:
        """Binning schema based on Bayesian blocks (from astropy).

        Computationally expensive for large data sets.

        Parameters
        ----------
        range: Optional[tuple]

        See also
        --------
        astropy.stats.histogram.bayesian_blocks
        astropy.stats.histogram.histogram
        """
        from astropy.stats.histogram import bayesian_blocks

        if range is not None:
            data = data[(data >= range[0]) & (data <= range[1])]
        edges = bayesian_blocks(data)
        return NumpyBinning(edges, **kwargs)

    def knuth_binning(data, range=None, **kwargs) -> StaticBinning:
        """Binning schema based on Knuth's rule (from astropy).

        Computationally expensive for large data sets.

        Parameters
        ----------
        data: arraylike
        range: Optional[tuple]

        See also
        --------
        astropy.stats.histogram.knuth_bin_width
        astropy.stats.histogram.histogram
        """
        # TODO: Could we possibly use it with FixedWidthBinning?
        from astropy.stats.histogram import knuth_bin_width

        if range is not None:
            data = data[(data >= range[0]) & (data <= range[1])]
        _, edges = knuth_bin_width(data, True)
        return NumpyBinning(edges, **kwargs)

    def scott_binning(data, range=None, **kwargs) -> StaticBinning:
        """Binning schema based on Scott's rule (from astropy).

        Parameters
        ----------
        data: arraylike
        range: Optional[tuple]

        See also
        --------
        astropy.stats.histogram.scott_bin_width
        astropy.stats.histogram.histogram
        """
        from astropy.stats.histogram import scott_bin_width

        if range is not None:
            data = data[(data >= range[0]) & (data <= range[1])]
        _, edges = scott_bin_width(data, True)
        return NumpyBinning(edges, **kwargs)

    def freedman_binning(data, range=None, **kwargs) -> StaticBinning:
        """Binning schema based on Freedman-Diaconis rule (from astropy).

        Parameters
        ----------
        data: arraylike
        range: Optional[tuple]

        See also
        --------
        astropy.stats.histogram.freedman_bin_width
        astropy.stats.histogram.histogram
        """
        # TODO: Could we possibly use it with FixedWidthBinning?
        from astropy.stats.histogram import freedman_bin_width

        if range is not None:
            data = data[(data >= range[0]) & (data <= range[1])]
        _, edges = freedman_bin_width(data, True)
        return NumpyBinning(edges, **kwargs)

    binning_methods["blocks"] = bayesian_blocks_binning
    binning_methods["knuth"] = knuth_binning
    binning_methods["scott"] = scott_binning
    binning_methods["freedman"] = freedman_binning
except Exception:
    pass  # astropy is not required


def ideal_bin_count(data, method: str = "default") -> int:
    """A theoretically ideal bin count.

    Parameters
    ----------
    data: array_likes
        Data to work on. Most methods don't use this.
    method: str
        Name of the method to apply, available values:
          - default (~sturges)
          - sqrt
          - sturges
          - doane
          - rice
        See https://en.wikipedia.org/wiki/Histogram for the description
    """
    n = data.size
    if n < 1:
        return 1
    if method == "default":
        if n <= 32:
            return 7
        else:
            return ideal_bin_count(data, "sturges")
    elif method == "sqrt":
        return int(np.ceil(np.sqrt(n)))
    elif method == "sturges":
        return int(np.ceil(np.log2(n)) + 1)
    elif method == "doane":
        if n < 3:
            return 1
        from scipy.stats import skew

        sigma = np.sqrt(6 * (n - 2) / (n + 1) * (n + 3))
        return int(np.ceil(1 + np.log2(n) + np.log2(1 + np.abs(skew(data)) / sigma)))
    elif method == "rice":
        return int(np.ceil(2 * np.power(n, 1 / 3)))


bincount_methods = ["default", "sturges", "rice", "sqrt", "doane"]


def as_binning(obj, copy: bool = False) -> BinningBase:
    """Ensure that an object is a binning

    Parameters
    ---------
    obj : BinningBase or array_like
        Can be a binning, numpy-like bins or full physt bins
    copy : If true, ensure that the returned object is independent
    """
    if isinstance(obj, BinningBase):
        if copy:
            return obj.copy()
        else:
            return obj
    else:
        bins = make_bin_array(obj)
        return StaticBinning(bins)
