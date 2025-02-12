import time
from eeclient.async_client import AsyncEESession
from eeclient.data import get_assets_async
import asyncio

# get sepal_headers from sepal
async_session = AsyncEESession(sepal_headers)


async def test_get_assets_async():
    return await get_assets_async(
        async_client=async_session,
        folder=f"projects/{async_session.project_id}/assets/",
    )


if __name__ == "__main__":
    start_time = time.time()
    results = asyncio.run(test_get_assets_async())
    end_time = time.time()
    duration = end_time - start_time
    print(results)
    print(f"Time taken: {duration} seconds")
