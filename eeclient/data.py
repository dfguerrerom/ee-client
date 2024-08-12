from typing import List, TypedDict, Union
from ee.ee_exception import EEException

import json
import ee
from ee import serializer
from ee import _cloud_api_utils

from ee.data import TileFetcher

from eeclient.client import Session


class MapTileOptions(TypedDict):
    """
    MapTileOptions defines the configuration for map tile generation.

    Keys:
        min (str or List[str]): Comma-separated numbers representing the values
            to map onto 00. It can be a string of comma-separated numbers
            (e.g., "1,2,3") or a list of strings. (e.g., ["1", "2", "3"]).
        max (str or List[str]): Comma-separated numbers representing the values
            to map onto FF. It can be a string of comma-separated numbers or
            a list of strings.
        gain (str or List[str]): Comma-separated numbers representing the gain
            to map onto 00-FF. It can be a string of comma-separated numbers or
            a list of strings.
        bias (str or List[str]): Comma-separated numbers representing the
            offset to map onto 00-FF. It can be a string of comma-separated
            numbers or a list of strings.
        gamma (str or List[str]): Comma-separated numbers representing the
            gamma correction factor. It can be a string of comma-separated
            numbers or a list of strings.
        palette (str): A string of comma-separated CSS-style color strings
            (single-band previews only).For example, 'FF0000,000000'.
        format (str): The desired map tile format.
    """

    min: Union[str, List[str]]
    max: Union[str, List[str]]
    gain: Union[str, List[str]]
    bias: Union[str, List[str]]
    gamma: Union[str, List[str]]
    palette: str
    format: str


def get_ee_image(
    ee_object: Union[ee.Image, ee.ImageCollection, ee.Feature, ee.FeatureCollection],
    vis_params: MapTileOptions = {},
):
    """Convert an Earth Engine object to a image request object"""

    def get_image_request(ee_image: ee.Image, vis_params={}):

        vis_image, request = ee_image._apply_visualization(vis_params)
        request["image"] = vis_image

        return request

    if isinstance(ee_object, ee.Image):
        return get_image_request(ee_object, vis_params)

    elif isinstance(ee_object, ee.ImageCollection):

        ee_image = ee_object.mosaic()
        return get_image_request(ee_image, vis_params)

    elif isinstance(ee_object, ee.Feature):
        ee_image = ee.FeatureCollection(ee_object).draw(
            color=(vis_params or {}).get("color", "000000")
        )
        return get_image_request(ee_image)

    elif isinstance(ee_object, ee.FeatureCollection):
        ee_image = ee_object.draw(color=(vis_params or {}).get("color", "000000"))
        return get_image_request(ee_image)

    else:
        raise ValueError("Invalid ee_object type")


def get_map_id(
    session: Session,
    ee_image: ee.Image,
    vis_params: MapTileOptions = None,
    bands: str = None,
    format: str = None,
):
    """Get the map id of an image

    Args:
        session: The session object
        ee_image: The image to get the map id of
        vis_params (Optional[MapTileOptions]): The visualization parameters,
            such as min/max values, gain, bias, gamma correction,
        bands: The bands to display
            palette, and format. Refer to the MapTileOptions type for details.
        format: A string describing an image file format that was passed to one
            of the functions in ee.data that takes image file formats
    """

    ee_image_request = get_ee_image(ee_image)

    # renname
    format_ = format

    url = "https://earthengine.googleapis.com/v1alpha/projects/{project}/maps"

    request_body = {
        "expression": serializer.encode(ee_image_request, for_cloud_api=True),
        "fileFormat": _cloud_api_utils.convert_to_image_file_format(format_),
        "bandIds": _cloud_api_utils.convert_to_band_list(bands),
    }

    visualization_options = _cloud_api_utils.convert_to_visualization_options(
        vis_params
    )

    if visualization_options:
        request_body["visualizationOptions"] = visualization_options

    request_body = json.dumps(request_body)

    response = session.rest_call("POST", url, data=request_body)
    map_name = response["name"]

    _tile_base_url = "https://earthengine.googleapis.com"
    version = "v1"

    url_format = "%s/%s/%s/tiles/{z}/{x}/{y}" % (
        _tile_base_url,
        version,
        map_name,
    )
    return {
        "mapid": map_name,
        "token": "",
        "tile_fetcher": TileFetcher(url_format, map_name=map_name),
    }


# def get_map_tile(map_name: str):

#     return TileLayer(
#         url=map_id["tile_fetcher"].url_format,
#         attribution="Google Earth Engine",
#         name="name",
#         max_zoom=24,
#     )


def get_info(session: Session, ee_object: ee.ComputedObject, workloadTag=None):
    """Get the info of an Earth Engine object"""

    data = {
        "expression": serializer.encode(ee_object),
        "workloadTag": workloadTag,
    }

    url = "https://earthengine.googleapis.com/v1/projects/{project}/value:compute"

    return session.rest_call("POST", url, data=data)["result"]


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
