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
from artemis.externals.physt import bin_utils
import numpy as np
import unittest


class TestMakeArray(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_make_from_numpy(self):
        arr = bin_utils.make_bin_array([0, 1, 2])
        assert np.array_equal(arr, [[0, 1], [1, 2]])

    def test_idempotent(self):
        arr = bin_utils.make_bin_array([[0, 1], [2, 3]])
        assert np.array_equal(arr, [[0, 1], [2, 3]])


class TestNumpyBinsWithMask(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_numpy_style(self):
        arr = np.array([1, 2, 3.1, 4])
        edges, mask = bin_utils.to_numpy_bins_with_mask(arr)
        assert np.array_equal(edges, [1, 2, 3.1, 4])
        assert np.array_equal(mask, [0, 1, 2])

    def test_consecutive(self):
        arr = np.array([[0, 1.1], [1.1, 2.1]])
        edges, mask = bin_utils.to_numpy_bins_with_mask(arr)
        assert np.array_equal(edges, [0, 1.1, 2.1])
        assert np.array_equal(mask, [0, 1])

    def test_unconsecutive(self):
        arr = np.array([[0, 1], [1.1, 2.1]])
        edges, mask = bin_utils.to_numpy_bins_with_mask(arr)
        assert np.array_equal(edges, [0, 1, 1.1, 2.1])
        assert np.array_equal(mask, [0, 2])

    def test_nonsense(self):
        arr = np.array([[0, 1], [0.1, 2.1]])
        with self.assertRaises(RuntimeError):
            bin_utils.to_numpy_bins_with_mask(arr)
        arr = np.array([[[0, 1], [0.1, 2.1]], [[0, 1], [0.1, 2.1]]])
        with self.assertRaises(RuntimeError):
            bin_utils.to_numpy_bins_with_mask(arr)


class TestValidation(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_rising(self):
        valid = [
            [[1, 2], [2, 3], [3, 4]],
            [[1, 2], [3, 4], [4, 5]]
        ]
        for sequence in valid:
            assert bin_utils.is_rising((np.array(sequence)))

        invalid = [
            [[2, 2], [2, 3], [3, 4]],
            [[1, 2], [1.7, 4], [4, 5]],
            [[1, 2], [3, 4], [2, 3]]
        ]
        for sequence in invalid:
            assert not bin_utils.is_rising((np.array(sequence)))

    def test_consecutive(self):
        valid = [
            [[1, 2], [2, 3], [3, 4]],
            [[1, 2], [2, 1.5], [1.5, 0.7]],
            [[1, 2.2], [2.2, 3], [3, 4]],
        ]
        for sequence in valid:
            assert bin_utils.is_consecutive((np.array(sequence)))

        invalid = [
            [[1, 2], [1.8, 3], [3, 4]],
            [[1, 2], [2.2, 3], [3, 4]]
        ]
        for sequence in invalid:
            assert not bin_utils.is_consecutive((np.array(sequence)))

if __name__ == "__main__":
    unittest.main()
