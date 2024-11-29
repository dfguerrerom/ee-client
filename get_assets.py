from concurrent.futures import ThreadPoolExecutor
import time
import json
import ee
import httpx
import asyncio
from typing import List, Union
from pathlib import Path
from eeclient.client import Session

ee.Initialize(project="ee-dfgm2006")

# Load your service account key
SERVICE_ACCOUNT_FILE = "/home/dguerrero/.config/earthengine/credentials"
SCOPES = ["https://www.googleapis.com/auth/earthengine.readonly"]


async def list_assets(token, folder):
    """Asynchronously list assets in a given folder using httpx with manually managed tokens."""
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient() as client:
        url = f"https://earthengine.googleapis.com/v1alpha/{folder}/:listAssets"
        response = await client.get(url, headers=headers)
        response.raise_for_status()

        result = response.json()

        if result == {}:
            return []

        return response.json()["assets"]


async def get_assets_async(folder: Union[str, Path] = "") -> List[dict]:
    """Get all the assets from the parameter folder asynchronously. Every nested asset will be displayed.
    Args:
        folder: the initial GEE folder
    Returns:
        the asset list. Each asset is a dict with 3 keys: 'type', 'name' and 'id'
    """

    # read credentials from json file
    credentials = json.loads(Path(SERVICE_ACCOUNT_FILE).read_text())

    session = Session(credentials=credentials, ee_project="sepal-ui-421413")
    token = session._get_access_token()

    folder = str(folder) if folder else "projects/earthengine-legacy/assets"
    asset_list = []
    queue = asyncio.Queue()
    await queue.put(folder)

    while not queue.empty():
        current_folder = await queue.get()
        assets = await list_assets(token, current_folder)

        for asset in assets:
            asset_list.append(
                {"type": asset["type"], "name": asset["name"], "id": asset["id"]}
            )
            if asset["type"] == "FOLDER":
                await queue.put(asset["name"])  # Enqueue subfolder for processing

    return asset_list


def get_assets_while(folder: Union[str, Path] = "") -> List[dict]:
    """Get all the assets from the parameter folder. Every nested asset will be displayed.

    Args:
        folder: the initial GEE folder

    Returns:
        the asset list. Each asset is a dict with 3 keys: 'type', 'name' and 'id'
    """
    # Convert folder to string and set default if necessary
    folder = (
        str(folder) if folder else f"projects/{ee.data._cloud_api_user_project}/assets/"
    )
    asset_list = []
    folders_to_process = [folder]  # Use a queue to handle folders

    while folders_to_process:
        current_folder = folders_to_process.pop(0)
        assets = ee.data.listAssets({"parent": current_folder})["assets"]

        for asset in assets:
            asset_list.append(asset)
            if asset["type"] == "FOLDER":
                folders_to_process.append(
                    asset["name"]
                )  # Queue subfolder for processing

    return asset_list


def get_assets_recursive(folder: Union[str, Path] = "") -> List[dict]:
    """Get all the assets from the parameter folder. every nested asset will be displayed.

    Args:
        folder: the initial GEE folder

    Returns:
        the asset list. each asset is a dict with 3 keys: 'type', 'name' and 'id'
    """
    # set the folder and init the list
    asset_list = []
    folder = str(folder) or f"projects/{ee.data._cloud_api_user_project}/assets/"

    def _recursive_get(folder, asset_list):

        # loop in the assets
        for asset in ee.data.listAssets({"parent": folder})["assets"]:
            asset_list += [asset]
            if asset["type"] == "FOLDER":
                asset_list = _recursive_get(asset["name"], asset_list)

        return asset_list

    return _recursive_get(folder, asset_list)


async def list_assets_concurrently(token, folders):
    headers = {"Authorization": f"Bearer {token}"}
    urls = [
        f"https://earthengine.googleapis.com/v1alpha/{folder}/:listAssets"
        for folder in folders
    ]

    async with httpx.AsyncClient() as client:
        tasks = (client.get(url, headers=headers) for url in urls)
        responses = await asyncio.gather(*tasks)
        return [
            response.json()["assets"]
            for response in responses
            if response.status_code == 200 and response.json().get("assets")
        ]


async def get_assets_async_concurrent(folder: str = "") -> List[dict]:
    credentials = json.loads(Path(SERVICE_ACCOUNT_FILE).read_text())
    session = Session(credentials=credentials, ee_project="sepal-ui-421413")
    token = session._get_access_token()

    folder_queue = asyncio.Queue()
    await folder_queue.put(folder)
    asset_list = []

    while not folder_queue.empty():
        current_folders = [
            await folder_queue.get() for _ in range(folder_queue.qsize())
        ]
        assets_groups = await list_assets_concurrently(token, current_folders)

        for assets in assets_groups:
            for asset in assets:
                asset_list.append(
                    {"type": asset["type"], "name": asset["name"], "id": asset["id"]}
                )
                if asset["type"] == "FOLDER":
                    await folder_queue.put(asset["name"])

    return asset_list


# To run the asynchronous function
async def main():
    folder = "projects/ee-dfgm2006"

    return await get_assets_async(folder)


async def list_assets_concurrent_2(folders):
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(executor, ee.data.listAssets, {"parent": folder})
            for folder in folders
        ]
        results = await asyncio.gather(*tasks)
        return results


async def get_assets_async_concurrent_2(
    folder: str = "projects/your-project/assets",
) -> list:
    folder_queue = asyncio.Queue()
    await folder_queue.put(folder)
    asset_list = []

    while not folder_queue.empty():
        current_folders = [
            await folder_queue.get() for _ in range(folder_queue.qsize())
        ]
        assets_groups = await list_assets_concurrent_2(current_folders)

        for assets in assets_groups:
            for asset in assets.get("assets", []):
                asset_list.append(
                    {"type": asset["type"], "name": asset["name"], "id": asset["id"]}
                )
                if asset["type"] == "FOLDER":
                    await folder_queue.put(asset["name"])

    return asset_list


if __name__ == "__main__":

    print("####################### Async #######################")
    start_time = time.time()
    asset_list = asyncio.run(main())
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")
    # print(asset_list)

    print("####################### While #######################")
    start_time = time.time()
    asset_list = get_assets_while()
    end_time = time.time()

    print(f"Execution time: {end_time - start_time} seconds")

    print("####################### Recursive #######################")
    start_time = time.time()
    asset_list = get_assets_recursive()
    end_time = time.time()

    print(f"Execution time: {end_time - start_time} seconds")

    print("####################### Async Concurrent #######################")
    start_time = time.time()
    asset_list = asyncio.run(get_assets_async_concurrent(folder="projects/ee-dfgm2006"))
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")

    print("####################### Async Concurrent 2 #######################")
    start_time = time.time()
    asset_list = asyncio.run(
        get_assets_async_concurrent_2(folder="projects/ee-dfgm2006/assets/")
    )
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")
