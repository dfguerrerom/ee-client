import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

from eeclient.exceptions import EERestException
from eeclient.logger import logger
from eeclient.typing import MapTileOptions
from eeclient.helpers import _get_ee_image, convert_asset_id_to_asset_name

if TYPE_CHECKING:
    from eeclient.async_client import AsyncEESession

from ee import serializer
from ee import _cloud_api_utils

from ee.image import Image
from ee.computedobject import ComputedObject

from ee.data import TileFetcher


async def get_map_id(
    async_client: "AsyncEESession",
    ee_image: Image,
    vis_params: Union[dict, MapTileOptions] = {},
    bands: Optional[str] = None,
    format: Optional[str] = None,
):
    """Async version of get_map_id.

    Gets the map id of an image.

    Args:
        async_client: The asynchronous session object.
        ee_image: The image to get the map id of.
        vis_params (Optional[MapTileOptions]): Visualization parameters,
            such as min/max values, gain, bias, gamma correction,
            palette, and format. See MapTileOptions for details.
        bands: The bands to display.
        format: A string describing an image file format passed to one of the
            functions in ee.data that takes image file formats.

    Returns:
        A dictionary with keys 'mapid', 'token', and 'tile_fetcher'.
    """
    ee_image_request = _get_ee_image(ee_image, vis_params=vis_params)

    # rename
    format_ = format

    url = "{earth_engine_api_url}/projects/{project}/maps"

    request_body = {
        "expression": serializer.encode(ee_image_request["image"], for_cloud_api=True),
        "fileFormat": _cloud_api_utils.convert_to_image_file_format(format_),
        "bandIds": _cloud_api_utils.convert_to_band_list(bands),
    }

    response = await async_client.rest_call("POST", url, data=request_body)
    map_name = response["name"]

    _tile_base_url = "https://earthengine.googleapis.com"
    version = "v1"

    url_format = f"{_tile_base_url}/{version}/{map_name}/tiles/{{z}}/{{x}}/{{y}}"
    return {
        "mapid": map_name,
        "token": "",
        "tile_fetcher": TileFetcher(url_format, map_name=map_name),
    }


async def get_info(
    async_client: "AsyncEESession",
    ee_object: Union[ComputedObject, None] = None,
    workloadTag=None,
    serialized_object=None,
):
    """Async version of get_info.

    Gets the info of an Earth Engine object.

    Args:
        async_client: The asynchronous session object.
        ee_object: The Earth Engine object (ComputedObject) to compute info from.
        workloadTag: An optional workload tag.
        serialized_object: A serialized representation of the object.

    Returns:
        The computed result.

    Raises:
        ValueError: If neither ee_object nor serialized_object is provided.
    """
    if not ee_object and not serialized_object:
        raise ValueError("Either ee_object or serialized_object must be provided")

    data = {
        "expression": serialized_object or serializer.encode(ee_object),
        "workloadTag": workloadTag,
    }

    url = "https://earthengine.googleapis.com/v1/projects/{project}/value:compute"

    response = await async_client.rest_call("POST", url, data=data)
    return response["result"]


async def get_asset(async_client: "AsyncEESession", ee_asset_id: str):
    """Async version of get_asset.

    Gets the asset info from the asset id.

    Args:
        async_client: The asynchronous session object.
        ee_asset_id: The asset id string.

    Returns:
        The asset info as returned by the API.
    """
    # I need first to set the project before converting the asset id to asset name
    ee_asset_id = async_client.set_url_project(ee_asset_id)
    url = "{earth_engine_api_url}/" + convert_asset_id_to_asset_name(ee_asset_id)
    try:
        return await async_client.rest_call("GET", url)
    except EERestException as e:
        if e.code == 404:
            logger.info(f"Asset: '{ee_asset_id}' not found")
            return None
        else:
            raise e
    except Exception as e:
        logger.error(f"Unexpected error while getting asset {ee_asset_id}: {e}")
        raise


async def list_assets_concurrently(async_client: "AsyncEESession", folders):
    """List assets concurrently.

    Args:
        async_client: The asynchronous session object.
        folders: A list of folder names (or identifiers) for which to list assets.

    Returns:
        A list of asset groups where each group is the list of assets in the folder.
    """
    urls = [
        f"https://earthengine.googleapis.com/v1alpha/{folder}/:listAssets"
        for folder in folders
    ]

    tasks = (async_client.rest_call("GET", url) for url in urls)
    responses = await asyncio.gather(*tasks)
    return [response["assets"] for response in responses if response.get("assets")]


async def get_assets_async(
    async_client: "AsyncEESession", folder: str = ""
) -> List[dict]:
    """Get all assets in a folder recursively (async version).

    Args:
        async_client: The asynchronous session object.
        folder: The starting folder name or id.

    Returns:
        A list of asset dictionaries containing type, name, and id.
    """
    folder_queue = asyncio.Queue()
    await folder_queue.put(folder)
    asset_list = []

    while not folder_queue.empty():
        current_folders = [
            await folder_queue.get() for _ in range(folder_queue.qsize())
        ]
        assets_groups = await list_assets_concurrently(async_client, current_folders)

        for assets in assets_groups:
            for asset in assets:
                asset_list.append(
                    {"type": asset["type"], "name": asset["name"], "id": asset["id"]}
                )
                if asset["type"] == "FOLDER":
                    await folder_queue.put(asset["name"])

    return asset_list


async def create_folder(
    async_client: "AsyncEESession", folder: Union[Path, str]
) -> Path:
    """Create a folder and its parents in Earth Engine if they don't exist.

    Args:
        async_client: The asynchronous session object.
        folder: The folder path (e.g. 'parent/child/grandchild').

    Raises:
        ValueError: If the folder path is empty or invalid.
    """

    # check if project path is passed, throw error if it is
    if str(folder).startswith("projects/"):
        raise ValueError("Folders should be relative to the project root")

    folder = str(folder)

    if not folder or not folder.strip("/"):
        raise ValueError("Folder path cannot be empty")

    # Clean and split the path
    folder = folder.strip("/")
    folders_to_create = []
    current = ""

    # Build list of folders to create
    for part in folder.split("/"):
        current = f"{current}/{part}" if current else part
        folder_id = f"projects/{{project}}/assets/{current}"

        try:
            logger.debug(f"Checking if folder exists: {current}, {folder_id}")
            await get_asset(async_client, folder_id)
            logger.debug(f"Folder exists: {current}")
        except Exception:
            folders_to_create.append(current)

    logger.debug(f"Creating folders: {folders_to_create}")

    for folder_id in folders_to_create:
        await async_client.rest_call(
            "POST",
            "{earth_engine_api_url}/projects/{project}/assets",
            params={"assetId": folder_id},
            data={"type": "FOLDER"},
        )

    return async_client.get_assets_folder() / Path(folder)
