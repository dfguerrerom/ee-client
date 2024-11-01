import asyncio
from .aioclient import AioEE


async def fetch_assets(aioee, parent, is_root=True):
    """Fetch assets from a specific parent directory, adjusting call based on level."""
    if is_root:
        request = aioee.ee_api.projects.listAssets(parent=parent)
    else:
        request = aioee.ee_api.projects.assets.listAssets(parent=parent)
    return await aioee.as_user(request)


async def explore_folders(aioee, initial_folder):
    """Explore folders and sub-folders to list all assets, adapting to root and nested."""

    queue = asyncio.Queue()
    await queue.put((initial_folder, True))  # (folder path, is_root)
    all_assets = []
    tasks = []

    while not queue.empty():
        current_folder, is_root = await queue.get()
        tasks.append(fetch_assets(aioee, current_folder, is_root))

        # Process a batch of folders concurrently
        if (
            queue.empty() or len(tasks) >= 10
        ):  # Adjust batch size based on rate limits and performance considerations
            responses = await asyncio.gather(*tasks)
            tasks = []  # Reset the task list for the next batch
            for response in responses:
                assets = response.get("assets", []) if response else []
                for asset in assets:
                    all_assets.append(
                        {
                            "type": asset["type"],
                            "name": asset["name"],
                            "id": asset["id"],
                        }
                    )
                    if asset["type"] == "FOLDER":
                        await queue.put((asset["name"], False))  # Non-root folders

    return all_assets
