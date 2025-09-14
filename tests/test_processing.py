import numpy as np
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processing import linear_regression

def test_linear_regression_constraints():
    # Simple increasing data
    x = np.array([0, 1, 2, 3])
    y = np.array([0, 0.5, 1.0, 1.0])
    intercept, slope = linear_regression(x, y)
    assert 0.0 <= intercept <= 1.0
    assert slope >= 0.0
    assert not np.isnan(intercept)
    assert not np.isnan(slope)

def test_linear_regression_constant():
    x = np.array([1, 1, 1, 1])
    y = np.array([0.5, 0.5, 0.5, 0.5])
    intercept, slope = linear_regression(x, y)
    assert 0.0 <= intercept <= 1.0
    assert slope >= 0.0

def test_linear_regression_nan_protection():
    x = np.array([])
    y = np.array([])
    intercept, slope = linear_regression(x, y)
    assert 0.0 <= intercept <= 1.0
    assert slope >= 0.0
    assert not np.isnan(intercept)
    assert not np.isnan(slope)

