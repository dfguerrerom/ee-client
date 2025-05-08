import logging
import ee

ee.Initialize()
import asyncio
from pathlib import Path
import random
import string
import sys
import pytest
from eeclient.data import (
    create_folder,
    delete_asset,
    delete_folder,
    get_asset,
    get_assets_async,
    get_info,
    get_map_id,
    list_assets_concurrently,
)
from eeclient.exceptions import EERestException
from eeclient.client import EESession

logger = logging.getLogger("eeclient")
sys.path.append("..")


# Helper to generate a unique identifier for each test run.
def random_hash(length=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


@pytest.mark.asyncio
async def test_create_folder(hash, sepal_headers):

    sepal_session = EESession(sepal_headers=sepal_headers)
    folder_id = await create_folder(
        sepal_session, folder=f"test_folder_{hash}/123/123/1"
    )

    # Get the folder and check if it exists
    asset = await get_asset(sepal_session, asset_id=folder_id)
    logger.debug(f"Folder: {asset}")

    assert asset is not None
    assert asset["id"] == folder_id
    assert asset["type"] == "FOLDER"

    # Clean up
    await delete_folder(sepal_session, folder_id=folder_id, recursive=True)


@pytest.mark.asyncio
async def test_delete_asset(sepal_headers):
    """Test deleting an asset from Earth Engine."""

    sepal_session = EESession(sepal_headers=sepal_headers)
    await create_folder(sepal_session, folder="1/2/3/4/5/65/")

    folder_to_delete = str(Path(await sepal_session.get_assets_folder()) / "1/")

    with pytest.raises(EERestException) as excinfo:
        await delete_asset(sepal_session, asset_id=folder_to_delete)

    assert excinfo.value.code == 400

    with pytest.raises(EERestException) as excinfo:
        await delete_folder(sepal_session, folder_id=folder_to_delete, recursive=False)
        logger.debug(f"Folder to delete: {excinfo}")

    assert excinfo.value.code == 400

    await delete_folder(sepal_session, folder_id=folder_to_delete, recursive=True)


@pytest.mark.asyncio
async def test_get_assets_async(sepal_headers):
    """
    Test the recursive asset listing.
    """
    session = EESession(sepal_headers=sepal_headers)
    folder_name = f"test_folder_{random_hash()}/async_test"
    folder_id = await create_folder(session, folder=folder_name)
    # Allow asset propagation.
    await asyncio.sleep(2)
    assets = await get_assets_async(session, folder=folder_id)
    assert isinstance(assets, list)
    print(f"Assets from get_assets_async: {assets}")
    # Clean up
    await delete_folder(session, folder_id=folder_id, recursive=True)


@pytest.mark.asyncio
async def test_get_map_id(sepal_headers):
    """
    Test get_map_id against a real Earth Engine image.
    """
    session = EESession(sepal_headers=sepal_headers)
    # Use a known public image (adjust if necessary)
    test_image = ee.image.Image(1)
    result = await get_map_id(
        session, test_image, vis_params={}, bands=None, format="png"
    )
    # Check that the result contains the expected keys.
    assert "mapid" in result
    assert "token" in result
    assert "tile_fetcher" in result


@pytest.mark.asyncio
async def test_get_info(sepal_headers):
    """
    Test get_info using a public Earth Engine image.
    """
    session = EESession(sepal_headers=sepal_headers)
    test_number = ee.ee_number.Number(1)
    result = await get_info(
        session, ee_object=test_number, workloadTag="integration_test"
    )
    assert result == 1
    assert result is not None


@pytest.mark.asyncio
async def test_create_and_get_asset(sepal_headers):
    """
    Create a folder asset and then retrieve it via get_asset.
    """
    session = EESession(sepal_headers=sepal_headers)
    folder_name = f"test_folder_{random_hash()}/subfolder"
    folder_id = await create_folder(session, folder=folder_name)

    # Allow some time for the asset to be fully registered
    await asyncio.sleep(2)
    asset = await get_asset(session, asset_id=folder_id)
    assert asset is not None, f"Asset not found: {folder_id}"
    assert asset["id"] == folder_id
    assert asset["type"] == "FOLDER"

    # Clean up
    await delete_folder(session, folder_id=folder_id, recursive=True)


@pytest.mark.asyncio
async def test_list_assets_concurrently(sepal_headers):
    """
    Create a folder, then list its assets concurrently.
    """
    session = EESession(sepal_headers=sepal_headers)
    folder_name = f"test_folder_{random_hash()}/list_test"
    folder_id = await create_folder(session, folder=folder_name)

    # Wait a moment for propagation.
    await asyncio.sleep(2)
    folders = [folder_id]
    assets_lists = await list_assets_concurrently(session, folders)
    assert isinstance(assets_lists, list)

    # Depending on your project, the list might be empty or not.
    print(f"Assets listed: {assets_lists}")
    # Clean up
    await delete_folder(session, folder_id=folder_id, recursive=True)


@pytest.mark.asyncio
async def test_delete_folder_recursive(sepal_headers):
    """
    Create a folder with a subfolder and test recursive deletion.
    """
    session = EESession(sepal_headers=sepal_headers)
    folder_name = f"test_folder_{random_hash()}/delete_folder_recursive"
    folder_id = await create_folder(session, folder=folder_name)
    # Create a subfolder inside the main folder.
    subfolder_name = f"{folder_name}/subfolder"
    subfolder_id = await create_folder(session, folder=subfolder_name)
    # Allow propagation.
    await asyncio.sleep(2)
    # Delete the folder recursively.
    await delete_folder(session, folder_id=folder_id, recursive=True)
    # Verify deletion: both the main folder and subfolder should be gone.
    asset_main = await get_asset(session, asset_id=folder_id)
    asset_sub = await get_asset(session, asset_id=subfolder_id)
    assert asset_main is None, f"Main folder {folder_id} still exists."
    assert asset_sub is None, f"Subfolder {subfolder_id} still exists."
