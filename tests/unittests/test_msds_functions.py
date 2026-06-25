import logging
import pytest
from unittest.mock import patch
from msds_monthly_to_cml.processing.msds_functions import get_metric_status


import pytest


# Provisional status values
@pytest.mark.parametrize("status", ["provisional", "PROVISIONAL", "Provisional", "prov", "PROV", "Prov"])
def test_get_metric_status_returns_prov(status):
    assert get_metric_status({"status": status}) == "prov"


# Actual status values
@pytest.mark.parametrize("status", ["actual", "ACTUAL", "Actual", "final", "FINAL", "Final"])
def test_get_metric_status_returns_act(status):
    assert get_metric_status({"status": status}) == "act"


# Status with surrounding whitespace
@pytest.mark.parametrize("status", ["  provisional  ", "  prov  ", "  actual  ", "  final  "])
def test_get_metric_status_strips_whitespace(status):
    assert get_metric_status({"status": status}) in ("prov", "act")


# Missing status key
def test_get_metric_status_raises_when_status_missing():
    with pytest.raises(KeyError, match="'status' is missing from config"):
        get_metric_status({})


# Unrecognised status values
@pytest.mark.parametrize("status", ["", "provisionall", "finall", "unknown", "act"])
def test_get_metric_status_raises_on_unrecognised_status(status):
    with pytest.raises(ValueError, match="Unrecognised status value"):
        get_metric_status({"status": status})


# Status is None explicitly
def test_get_metric_status_raises_when_status_is_none():
    with pytest.raises(AttributeError):
        get_metric_status({"status": None})
