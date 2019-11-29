# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#   This module includes code from the Physt Project
#
#   (C) Jan Pipek, 2016-9, MIT licence
#   See https://github.com/janpipek/physt
import sys
import os
sys.path = [os.path.join(os.path.dirname(__file__), "..")] + sys.path
# from physt.histogram1d import Histogram1D
from artemis.externals.physt import histogram
import numpy as np
import unittest


class TestNumpyBins(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_nbin(self):
        arr = np.random.rand(100)
        hist = histogram(arr, bins=15)
        assert hist.bin_count == 15
        assert np.isclose(hist.bin_right_edges[-1], arr.max())
        assert np.isclose(hist.bin_left_edges[0], arr.min())

    def test_edges(self):
        arr = np.arange(0, 1, 0.01)
        hist = histogram(arr, np.arange(0.1, 0.8001, 0.1))
        assert np.allclose(hist.numpy_bins, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8])
        assert hist.underflow == 10
        assert hist.overflow == 19

    def test_range(self):
        arr = np.arange(0, 1.00, 0.01)
        hist = histogram(arr, 10, range=(0.5, 1.0))
        assert hist.bin_count == 10
        assert hist.bin_left_edges[0] == 0.5
        assert hist.bin_right_edges[-1] == 1.0
        assert hist.overflow == 0
        assert hist.underflow == 50
        assert hist.total == 50

        hist = histogram(arr, bins=10, range=(0.5, 1.0), keep_missed=False)
        assert hist.total == 50
        assert np.isnan(hist.underflow)
        assert np.isnan(hist.overflow)

    def test_metadata(self):
        arr = np.arange(0, 1.00, 0.01)
        hist = histogram(arr, name="name", title="title", axis_name="axis_name")
        assert hist.name == "name"
        assert hist.title == "title"
        assert hist.axis_names == ("axis_name",)


if __name__ == "__main__":
    unittest.main()
