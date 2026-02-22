"""Tests for pipeline.utils module."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pytest

from pipeline.utils import unpack_location


class TestUnpackLocation:
    def test_python_list(self):
        x, y = unpack_location([60.0, 40.0])
        assert x == 60.0
        assert y == 40.0

    def test_numpy_array(self):
        x, y = unpack_location(np.array([75.3, 30.1]))
        assert abs(x - 75.3) < 1e-6
        assert abs(y - 30.1) < 1e-6

    def test_none_returns_none(self):
        x, y = unpack_location(None)
        assert x is None
        assert y is None

    def test_nan_returns_none(self):
        x, y = unpack_location(float("nan"))
        assert x is None
        assert y is None

    def test_single_element_list(self):
        x, y = unpack_location([50.0])
        assert x == 50.0
        assert y is None

    def test_three_element_list(self):
        # Shot end_location has 3 elements — only first two returned
        x, y = unpack_location([120.0, 36.0, 1.5])
        assert x == 120.0
        assert y == 36.0

    def test_empty_list(self):
        x, y = unpack_location([])
        assert x is None
        assert y is None
