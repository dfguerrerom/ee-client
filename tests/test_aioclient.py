import asyncio
from eeclient.aiodata import explore_folders
from eeclient.aioclient import AioEE


async def test_get_assets_async_concurrent():

    async with AioEE() as aioee:
        # Load the Earth Engine API discovery document

        initial_folder = "projects/ee-dfgm2006"
        asset_list = await explore_folders(aioee, initial_folder)
        return asset_list


# Run the test
print(asyncio.run(test_get_assets_async_concurrent()))
