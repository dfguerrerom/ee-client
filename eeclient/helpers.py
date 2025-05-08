import os
import requests
import logging
from eeclient.models import SepalHeaders, SepalUser
from typing import Union

from ee.imagecollection import ImageCollection
from ee.feature import Feature
from ee.featurecollection import FeatureCollection
from ee.image import Image
from eeclient.models import MapTileOptions
from ee.data import convert_asset_id_to_asset_name  # type: ignore: it will be imported from another moduel

logger = logging.getLogger("eeclient")


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


def parse_cookie_string(cookie_string):
    cookies = {}
    for pair in cookie_string.split(";"):
        key_value = pair.strip().split("=", 1)
        if len(key_value) == 2:
            key, value = key_value
            cookies[key] = value
    return cookies


def get_sepal_headers_from_auth():
    sepal_user = os.getenv("LOCAL_SEPAL_USER")
    sepal_password = os.getenv("LOCAL_SEPAL_PASSWORD")
    sepal_host = os.getenv("SEPAL_HOST")

    if not sepal_user or not sepal_password or not sepal_host:
        raise ValueError(
            "LOCAL_SEPAL_USER, LOCAL_SEPAL_PASSWORD, and SEPAL_HOST must be set"
        )

    session = requests.Session()
    session.verify = False

    creds_response = session.post(
        f"https://{sepal_host}/api/user/login",
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "No-Auth-Challenge": "true",
        },
        auth=(sepal_user, sepal_password),
        data="",
    )

    creds_response.raise_for_status()

    logger.debug(f"Authentication successful. Cookies: {session.cookies}")
    logger.debug(f"Response: {creds_response.json()}")

    sepal_user_obj = SepalUser.model_validate(creds_response.json())

    # Replace the project_id with the one from the environment
    sepal_user_obj.google_tokens.project_id = "sepal-ui-421413"

    cookies_dict = {cookie.name: cookie.value for cookie in session.cookies}

    sepal_headers = {
        "cookie": cookies_dict,
        "sepal-user": sepal_user_obj,
    }

    return SepalHeaders.model_validate(sepal_headers)
