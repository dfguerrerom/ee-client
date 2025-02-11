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


# async def list_assets(token, folder):
#     """Asynchronously list assets in a given folder using httpx with manually managed tokens."""
#     headers = {"Authorization": f"Bearer {token}"}
#     async with httpx.AsyncClient() as client:
#         url = f"https://earthengine.googleapis.com/v1alpha/{folder}/:listAssets"
#         response = await client.get(url, headers=headers)
#         response.raise_for_status()

#         result = response.json()

#         if result == {}:
#             return []

#         return response.json()["assets"]


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
    session = Session()
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


# async def list_assets_concurrent_2(folders):
#     with ThreadPoolExecutor() as executor:
#         loop = asyncio.get_running_loop()
#         tasks = [
#             loop.run_in_executor(executor, ee.data.listAssets, {"parent": folder})
#             for folder in folders
#         ]
#         results = await asyncio.gather(*tasks)
#         return results


# async def get_assets_async_concurrent_2(
#     folder: str = "projects/your-project/assets",
# ) -> list:
#     folder_queue = asyncio.Queue()
#     await folder_queue.put(folder)
#     asset_list = []

#     while not folder_queue.empty():
#         current_folders = [
#             await folder_queue.get() for _ in range(folder_queue.qsize())
#         ]
#         assets_groups = await list_assets_concurrent_2(current_folders)

#         for assets in assets_groups:
#             for asset in assets.get("assets", []):
#                 asset_list.append(
#                     {"type": asset["type"], "name": asset["name"], "id": asset["id"]}
#                 )
#                 if asset["type"] == "FOLDER":
#                     await folder_queue.put(asset["name"])

#     return asset_list


if __name__ == "__main__":

    print("####################### Async Concurrent #######################")
    start_time = time.time()
    asset_list = asyncio.run(get_assets_async_concurrent(folder="projects/ee-dfgm2006"))
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")

    # print("####################### Async Concurrent 2 #######################")
    # start_time = time.time()
    # asset_list = asyncio.run(
    #     get_assets_async_concurrent_2(folder="projects/ee-dfgm2006/assets/")
    # )
    # end_time = time.time()
    # print(f"Execution time: {end_time - start_time} seconds")
