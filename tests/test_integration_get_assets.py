import os
import pytest
import time
import logging
from unittest.mock import patch

from eeclient.helpers import get_sepal_headers_from_auth
from eeclient.client import EESession
from eeclient.data import get_assets_async

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_real_get_assets_cache():
    """Integration test against a real SEPAL server.

    Requires the following environment variables to be set:
      - SEPAL_HOST
      - LOCAL_SEPAL_USER
      - LOCAL_SEPAL_PASSWORD

    The test will be skipped if those are not available.
    """
    sepal_host = os.getenv("SEPAL_HOST")
    sepal_user = os.getenv("LOCAL_SEPAL_USER")
    sepal_password = os.getenv("LOCAL_SEPAL_PASSWORD")

    if not (sepal_host and sepal_user and sepal_password):
        pytest.skip("SEPAL_HOST, LOCAL_SEPAL_USER or LOCAL_SEPAL_PASSWORD not set")

    sepal_headers = get_sepal_headers_from_auth(
        sepal_user=sepal_user, sepal_password=sepal_password, sepal_host=sepal_host
    )

    session = await EESession.create(sepal_headers=sepal_headers)

    try:
        folder = await session.get_assets_folder()

        assert len(session._assets_cache._cache) == 0

        logger.info(f"Making first call to get_assets_async({folder})")
        start_time = time.time()
        assets = await get_assets_async(session, folder)
        first_call_duration = time.time() - start_time
        logger.info(
            f"First call completed in {first_call_duration:.3f}s, "
            f"retrieved {len(assets)} assets"
        )

        assert isinstance(assets, list)

        key = session._assets_cache.make_cache_key(folder)
        assert key in session._assets_cache._cache
        logger.debug(f"Cache entry created with key: {key[:16]}...")

        cache_entry = session._assets_cache._cache[key]
        assert cache_entry.value is not None
        assert cache_entry.task is None
        logger.debug("Cache entry contains value (no pending task)")

        logger.info("Making second call to get_assets_async (should use cache)")

        with patch("eeclient.data._list_assets_concurrently") as mock_list:
            start_time = time.time()
            assets2 = await get_assets_async(session, folder)
            second_call_duration = time.time() - start_time
            logger.info(f"Second call completed in {second_call_duration:.3f}s")

            assert (
                mock_list.call_count == 0
            ), "Network call should NOT happen for cached result"
            logger.info("Confirmed: NO network call made (mock not called)")

        assert assets2 == assets
        logger.debug("Results are identical")

        speedup = first_call_duration / second_call_duration
        logger.info(f"Cache speedup: {speedup:.1f}x faster")
        assert speedup > 10, f"Cache should be much faster! Got only {speedup:.1f}x"

    finally:
        await session.aclose()
