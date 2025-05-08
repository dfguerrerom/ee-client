import asyncio
from collections import defaultdict
import logging
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

from eeclient.exceptions import EERestException
from eeclient.models import MapTileOptions
from eeclient.helpers import _get_ee_image, convert_asset_id_to_asset_name

if TYPE_CHECKING:
    from eeclient.client import EESession

from ee import serializer
from ee import _cloud_api_utils

from ee.image import Image
from ee.computedobject import ComputedObject

from ee.data import TileFetcher

logger = logging.getLogger("eeclient")


async def get_map_id(
    client: "EESession",
    ee_image: Image,
    vis_params: Union[dict, MapTileOptions] = {},
    bands: Optional[str] = None,
    format: Optional[str] = None,
):
    """Async version of get_map_id.

    Gets the map id of an image.

    Args:
        client: The asynchronous session object.
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

    response = await client.rest_call("POST", url, data=request_body)
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
    client: "EESession",
    ee_object: Union[ComputedObject, None] = None,
    workloadTag=None,
    serialized_object=None,
):
    """Async version of get_info.

    Gets the info of an Earth Engine object.

    Args:
        client: The asynchronous session object.
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

    response = await client.rest_call("POST", url, data=data)
    return response["result"]


async def get_asset(
    client: "EESession", asset_id: str, not_exists_ok: bool = True
) -> Optional[dict]:
    """Async version of get_asset.

    Gets the asset info from the asset id.

    Args:
        client: The asynchronous session object.
        ee_asset_id: The asset id string.
        not_exists_ok: Whether to return None if the asset is not found. Otherwise, raise an exception.
    Returns:
        The asset info or None if the asset is not found.
    """
    # I need first to set the project before converting the asset id to asset name
    asset_id = client.set_url_project(asset_id)
    logger.debug(f"Getting asset: {asset_id}")
    url = "{earth_engine_api_url}/" + convert_asset_id_to_asset_name(asset_id)
    try:
        return await client.rest_call("GET", url)
    except EERestException as e:
        if e.code == 404:
            logger.info(f"Asset: '{asset_id}' not found")
            if not_exists_ok:
                return None
            raise
        else:
            raise e
    except Exception as e:
        logger.error(f"Unexpected error while getting asset {asset_id}: {e}")
        raise


async def list_assets_concurrently(client: "EESession", folders):
    """List assets concurrently.

    Args:
        client: The asynchronous session object.
        folders: A list of folder names (or identifiers) for which to list assets.

    Returns:
        A list of asset groups where each group is the list of assets in the folder.
    """
    urls = [
        f"https://earthengine.googleapis.com/v1alpha/{folder}/:listAssets"
        for folder in folders
    ]

    tasks = (client.rest_call("GET", url) for url in urls)
    responses = await asyncio.gather(*tasks)
    return [response["assets"] for response in responses if response.get("assets")]


async def get_assets_async(client: "EESession", folder: str = "") -> List[dict]:
    """Get all assets in a folder recursively (async version).

    Args:
        client: The asynchronous session object.
        folder: The starting folder name or id.

    Returns:

    """
    folder_queue = asyncio.Queue()
    await folder_queue.put(folder)
    asset_list = []

    while not folder_queue.empty():
        current_folders = [
            await folder_queue.get() for _ in range(folder_queue.qsize())
        ]
        assets_groups = await list_assets_concurrently(client, current_folders)

        for assets in assets_groups:
            for asset in assets:
                asset_list.append(
                    {"type": asset["type"], "name": asset["name"], "id": asset["id"]}
                )
                if asset["type"] == "FOLDER":
                    await folder_queue.put(asset["name"])

    return asset_list


async def create_folder(client: "EESession", folder: Union[Path, str]) -> str:
    """Create a folder and its parents in Earth Engine if they don't exist.

    Args:
        client: The asynchronous session object.
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

    full_path = str(await client.get_assets_folder() / Path(folder))

    if asset := await get_asset(client, full_path):
        logger.debug(f"Folder already exists: {full_path}")
        return full_path

    # Clean and split the path
    folder = folder.strip("/")
    folders_to_create = []
    current = ""

    # Build list of folders to create
    for part in folder.split("/"):
        current = f"{current}/{part}" if current else part
        folder_id = f"projects/{{project}}/assets/{current}"

        logger.debug(f"Checking if folder exists: {current}, {folder_id}")
        asset = await get_asset(client, folder_id)
        if asset:
            continue
        else:
            folders_to_create.append(current)

    logger.debug(f"Creating folders: {folders_to_create}")

    for folder_id in folders_to_create:
        await client.rest_call(
            "POST",
            "{earth_engine_api_url}/projects/{project}/assets",
            params={"assetId": folder_id},
            data={"type": "FOLDER"},
        )

    return full_path


async def delete_asset(client, asset_id: Union[str, Path]) -> None:
    """Delete an asset from Earth Engine."""

    asset_id = str(asset_id)
    url = "{earth_engine_api_url}/" + convert_asset_id_to_asset_name(asset_id)

    try:
        await client.rest_call("DELETE", url)
        logger.info(f"Asset deleted: {asset_id}")
    except EERestException as e:
        logger.error(f"Error deleting asset {asset_id}: {e}")
        if e.code == 404:
            logger.debug(f"Asset: '{asset_id}' not found")
            return
        else:
            raise e
    except Exception as e:
        logger.error(f"Unexpected error while deleting asset {asset_id}: {e}")
        raise


async def delete_folder(
    client, folder_id: Union[str, Path], recursive: bool = False
) -> None:
    """Delete a folder asset. If recursive is True, first delete all child assets asynchronously.

    Args:
        client: The Earth Engine session object.
        folder: The folder asset id or path.
        recursive: Whether to delete child assets recursively.
    """
    logger.debug(f"Deleting folder: {folder_id}, recursive={recursive}")
    folder_id = str(folder_id)
    if recursive:
        logger.debug(f"Recursively deleting folder: {folder_id}")
        assets = await get_assets_async(client, folder_id)
        if assets:
            depth_to_assets = defaultdict(list)
            for asset in assets:
                depth = asset["id"].count("/")
                depth_to_assets[depth].append(asset)

            # Process deletion from the deepest assets to the shallowest.
            for depth in sorted(depth_to_assets.keys(), reverse=True):
                tasks = [
                    asyncio.create_task(delete_asset(client, asset["id"]))
                    for asset in depth_to_assets[depth]
                ]
                # Await deletion of all assets at the current depth level.
                await asyncio.gather(*tasks)
                logger.info(f"Deleted all assets at depth {depth}")

        await delete_asset(client, folder_id)
    else:
        await delete_asset(client, folder_id)
