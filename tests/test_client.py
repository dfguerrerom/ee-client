import pytest
from eeclient.async_client import EESession
from eeclient.data import getInfo
import ee

ee.Initialize()

session = EESession(test=True)


def test_get_info():

    session = EESession(test=True)
    computed_object = ee.ee_number.Number(1).add(1)

    assert getInfo(session, computed_object) == 2
    assert computed_object.getInfo() == 2
    assert getInfo(session, computed_object) == computed_object.getInfo()
