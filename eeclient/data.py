from ee.ee_exception import EEException

import json
import ee
from ee import serializer
from ee import _cloud_api_utils

from ee.data import TileFetcher
from ipyleaflet import TileLayer

from eeclient.client import Session


def get_map_id(session: Session, ee_image: ee.Image):

    url = "https://earthengine.googleapis.com/v1alpha/projects/{project}/maps"

    request_body = {
        "expression": serializer.encode(ee_image, for_cloud_api=True),
        "fileFormat": _cloud_api_utils.convert_to_image_file_format(None),
        "bandIds": _cloud_api_utils.convert_to_band_list(None),
    }

    request_body = json.dumps(request_body)

    response = session.rest_call("POST", url, data=request_body)
    map_name = response["name"]

    return map_name


def get_map_tile(map_name: str):

    _tile_base_url = "https://earthengine.googleapis.com"
    version = "v1"

    url_format = "%s/%s/%s/tiles/{z}/{x}/{y}" % (
        _tile_base_url,
        version,
        map_name,
    )

    map_id = {
        "mapid": map_name,
        "token": "",
        "tile_fetcher": TileFetcher(url_format, map_name=map_name),
    }

    return TileLayer(
        url=map_id["tile_fetcher"].url_format,
        attribution="Google Earth Engine",
        name="name",
        max_zoom=24,
    )


def get_info(session: Session, ee_object, workloadTag=None):
    """Get the info of an Earth Engine object"""

    data = {
        "expression": serializer.encode(ee_object),
        "workloadTag": workloadTag,
    }

    url = "https://earthengine.googleapis.com/v1/projects/{project}/value:compute"

    return session.rest_call("POST", url, data=data)


def get_asset(session: Session, ee_asset_id: str):
    """Get the asset info from the asset id"""

    url = (
        "https://earthengine.googleapis.com/v1alpha/projects/{project}/assets/"
        + ee_asset_id
    )

    return session.rest_call("GET", url)


class EERestException(EEException):
    def __init__(self, error):
        self.message = error.get("message", "EE responded with an error")
        super().__init__(self.message)
        self.code = error.get("code", -1)
        self.status = error.get("status", "UNDEFINED")
        self.details = error.get("details")


getInfo = get_info
getAsset = get_asset
getMapTile = get_map_tile
