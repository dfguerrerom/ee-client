from typing import TYPE_CHECKING

from eeclient.typing import MapTileOptions

if TYPE_CHECKING:
    from eeclient.client import EESession

from typing import Optional, Union

import ee
from ee import serializer
from ee import _cloud_api_utils

from ee.image import Image
from ee.imagecollection import ImageCollection
from ee.feature import Feature
from ee.featurecollection import FeatureCollection
from ee.computedobject import ComputedObject

from ee.data import TileFetcher


def _get_ee_image(
    ee_object: Union[Image, ImageCollection, Feature, FeatureCollection],
    vis_params: Union[MapTileOptions, dict] = {},
):
    """Convert an Earth Engine object to a image request object"""

    def get_image_request(ee_image: Image, vis_params={}):

        vis_image, request = ee_image._apply_visualization(vis_params)
        request["image"] = vis_image

        return request

    if isinstance(ee_object, Image):
        return get_image_request(ee_object, vis_params)

    elif isinstance(ee_object, ImageCollection):

        ee_image = ee_object.mosaic()
        return get_image_request(ee_image, vis_params)

    elif isinstance(ee_object, Feature):
        ee_image = FeatureCollection(ee_object).draw(
            color=(vis_params or {}).get("color", "000000")
        )
        return get_image_request(ee_image)

    elif isinstance(ee_object, FeatureCollection):
        ee_image = ee_object.draw(color=(vis_params or {}).get("color", "000000"))
        return get_image_request(ee_image)

    else:
        raise ValueError("Invalid ee_object type")


def get_map_id(
    session: "EESession",
    ee_image: Image,
    vis_params: Union[dict, MapTileOptions] = {},
    bands: Optional[str] = None,
    format: Optional[str] = None,
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

    ee_image_request = _get_ee_image(ee_image, vis_params=vis_params)

    # rename
    format_ = format

    url = "{EARTH_ENGINE_API_URL}/projects/{project}/maps"

    request_body = {
        "expression": serializer.encode(ee_image_request["image"], for_cloud_api=True),
        "fileFormat": _cloud_api_utils.convert_to_image_file_format(format_),
        "bandIds": _cloud_api_utils.convert_to_band_list(bands),
    }

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


def get_info(
    session: "EESession",
    ee_object: Union[ComputedObject, None] = None,
    workloadTag=None,
    serialized_object=None,
):
    """Get the info of an Earth Engine object"""

    if not ee_object and not serialized_object:
        raise ValueError("Either ee_object or serialized_object must be provided")

    data = {
        "expression": serialized_object or serializer.encode(ee_object),
        "workloadTag": workloadTag,
    }
    # request_body = json.dumps(data)

    url = "https://earthengine.googleapis.com/v1/projects/{project}/value:compute"

    return session.rest_call("POST", url, data=data)["result"]


def get_asset(session: "EESession", ee_asset_id: str):
    """Get the asset info from the asset id"""

    url = "{EARTH_ENGINE_API_URL}/projects/{project}/assets/" + ee_asset_id

    return session.rest_call("GET", url)


getInfo = get_info
getAsset = get_asset
