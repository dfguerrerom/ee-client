import os
import requests
import logging
from eeclient.models import SepalHeaders, SepalUser
from typing import Optional, Union

from ee.imagecollection import ImageCollection
from ee.feature import Feature
from ee.featurecollection import FeatureCollection
from ee.image import Image
from eeclient.models import MapTileOptions
from ee.data import convert_asset_id_to_asset_name  # noqa: F401

log = logging.getLogger("eeclient")

# Hosts that legitimately serve self-signed certs in local development.
# TLS verification is skipped for these (or via EECLIENT_INSECURE_TLS) only —
# never for arbitrary or production hosts.
_LOCAL_INSECURE_HOSTS = {"host.docker.internal"}


def should_verify_tls(host: Optional[str] = None) -> bool:
    """Return whether TLS certificates should be verified for ``host``.

    Secure by default. Verification is skipped only for known local-dev hosts
    that serve self-signed certs, or when the ``EECLIENT_INSECURE_TLS`` env var
    is explicitly truthy. It is never disabled for arbitrary/production hosts.
    """
    if os.getenv("EECLIENT_INSECURE_TLS", "").strip().lower() in {"1", "true", "yes"}:
        return False
    if host and host in _LOCAL_INSECURE_HOSTS:
        return False
    return True


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


def get_sepal_headers_from_auth(
    sepal_user: Optional[str] = None,
    sepal_password: Optional[str] = None,
    sepal_host: Optional[str] = None,
) -> SepalHeaders:

    log.debug("Getting SEPAL headers from authentication")

    sepal_user = sepal_user or os.getenv("LOCAL_SEPAL_USER")
    sepal_password = sepal_password or os.getenv("LOCAL_SEPAL_PASSWORD")
    sepal_host = sepal_host or os.getenv("SEPAL_HOST")

    if not sepal_user or not sepal_password or not sepal_host:
        raise ValueError(
            "LOCAL_SEPAL_USER, LOCAL_SEPAL_PASSWORD, and SEPAL_HOST must be set"
        )

    session = requests.Session()
    session.verify = should_verify_tls(sepal_host)

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

    sepal_user_obj = SepalUser.model_validate(creds_response.json())
    log.debug("Authentication successful for user '%s'", sepal_user_obj.username)

    cookies_dict = {cookie.name: cookie.value for cookie in session.cookies}

    sepal_headers = {
        "cookie": cookies_dict,
        "sepal-user": sepal_user_obj,
    }

    return SepalHeaders.model_validate(sepal_headers)
