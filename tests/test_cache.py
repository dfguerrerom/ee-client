import asyncio
import pytest
import time
from unittest.mock import MagicMock, patch

from eeclient.cache import ResponseCache, CacheEntry
from eeclient.data import get_assets_async


# CacheEntry Tests


def test_cache_entry_with_value():
    entry = CacheEntry(value="test_value")
    assert entry.value == "test_value"
    assert entry.error is None
    assert entry.task is None
    assert entry.get_result() == "test_value"


def test_cache_entry_with_error():
    error = ValueError("test error")
    entry = CacheEntry(error=error)
    assert entry.value is None
    assert entry.error == error

    with pytest.raises(ValueError, match="test error"):
        entry.get_result()


def test_cache_entry_ttl_expiry():
    entry = CacheEntry(value="test")
    assert not entry.is_expired(ttl=10.0)

    entry.timestamp = time.time() - 15.0
    assert entry.is_expired(ttl=10.0)


# ResponseCache Tests


@pytest.mark.asyncio
async def test_cache_basic_caching():
    cache = ResponseCache(ttl=10.0, max_size=100)
    call_count = 0

    async def fetch_data(value):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return f"result_{value}"

    key = cache.make_cache_key("test_arg")

    result1 = await cache.get_or_fetch(key, fetch_data, "test_arg")
    result2 = await cache.get_or_fetch(key, fetch_data, "test_arg")

    assert result1 == "result_test_arg"
    assert result2 == "result_test_arg"
    assert call_count == 1


@pytest.mark.asyncio
async def test_cache_ttl_expiry():
    cache = ResponseCache(ttl=0.1, max_size=100)
    call_count = 0

    async def fetch_data(value):
        nonlocal call_count
        call_count += 1
        return f"result_{value}"

    key = cache.make_cache_key("test_arg")

    result1 = await cache.get_or_fetch(key, fetch_data, "test_arg")
    assert result1 == "result_test_arg"
    assert call_count == 1

    await asyncio.sleep(0.15)

    result2 = await cache.get_or_fetch(key, fetch_data, "test_arg")
    assert result2 == "result_test_arg"
    assert call_count == 2


@pytest.mark.asyncio
async def test_cache_concurrent_request_deduplication():
    cache = ResponseCache(ttl=10.0, max_size=100)
    call_count = 0

    async def fetch_data(value):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return f"result_{value}"

    key = cache.make_cache_key("test_arg")

    tasks = [
        asyncio.create_task(cache.get_or_fetch(key, fetch_data, "test_arg"))
        for _ in range(10)
    ]

    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

    assert all(r == "result_test_arg" for r in results)
    assert call_count == 1


@pytest.mark.asyncio
async def test_cache_different_params_different_cache():
    cache = ResponseCache(ttl=10.0, max_size=100)
    call_count = 0

    async def fetch_data(value):
        nonlocal call_count
        call_count += 1
        return f"result_{value}"

    key1 = cache.make_cache_key("arg1")
    key2 = cache.make_cache_key("arg2")

    result1 = await cache.get_or_fetch(key1, fetch_data, "arg1")
    result2 = await cache.get_or_fetch(key2, fetch_data, "arg2")

    assert result1 == "result_arg1"
    assert result2 == "result_arg2"
    assert call_count == 2


@pytest.mark.asyncio
async def test_cache_error_caching():
    cache = ResponseCache(ttl=10.0, max_size=100)
    call_count = 0

    async def fetch_data_with_error():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        raise ValueError("API error")

    key = cache.make_cache_key("error_test")

    with pytest.raises(ValueError, match="API error"):
        await cache.get_or_fetch(key, fetch_data_with_error)

    assert call_count == 1

    with pytest.raises(ValueError, match="API error"):
        await cache.get_or_fetch(key, fetch_data_with_error)

    assert call_count == 1


@pytest.mark.asyncio
async def test_cache_lru_eviction():
    cache = ResponseCache(ttl=10.0, max_size=3)

    async def fetch_data(value):
        return f"result_{value}"

    for i in range(5):
        key = cache.make_cache_key(f"arg_{i}")
        await cache.get_or_fetch(key, fetch_data, f"arg_{i}")

    assert len(cache._cache) == 3


@pytest.mark.asyncio
async def test_cache_clear():
    cache = ResponseCache(ttl=10.0, max_size=100)

    async def fetch_data(value):
        return f"result_{value}"

    key = cache.make_cache_key("test")
    await cache.get_or_fetch(key, fetch_data, "test")

    assert len(cache._cache) == 1

    cache.clear()

    assert len(cache._cache) == 0


def test_cache_key_consistency():
    cache = ResponseCache()

    key1 = cache.make_cache_key("folder1", param1="value1", param2="value2")
    key2 = cache.make_cache_key("folder1", param2="value2", param1="value1")

    assert key1 == key2


# get_assets_async Caching Tests


@pytest.mark.asyncio
async def test_get_assets_caching():
    """Test that identical get_assets calls use cache."""
    mock_client = MagicMock()
    mock_client._assets_cache = ResponseCache(ttl=10.0, max_size=100)

    call_count = 0

    async def mock_list_assets(client, folders):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return [
            [
                {"type": "IMAGE", "name": "test_image", "id": "test_id_1"},
                {"type": "IMAGE", "name": "test_image2", "id": "test_id_2"},
            ]
        ]

    with patch("eeclient.data._list_assets_concurrently", new=mock_list_assets):
        result1 = await get_assets_async(mock_client, "projects/test/assets/folder")
        assert len(result1) == 2
        assert call_count == 1

        result2 = await get_assets_async(mock_client, "projects/test/assets/folder")
        assert result1 == result2
        assert call_count == 1


@pytest.mark.asyncio
async def test_get_assets_concurrent_deduplication():
    """Test that concurrent identical requests are deduplicated."""
    mock_client = MagicMock()
    mock_client._assets_cache = ResponseCache(ttl=10.0, max_size=100)

    call_count = 0

    async def mock_list_assets(client, folders):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return [[{"type": "IMAGE", "name": "test_image", "id": "test_id"}]]

    with patch("eeclient.data._list_assets_concurrently", new=mock_list_assets):
        tasks = [
            asyncio.create_task(
                get_assets_async(mock_client, "projects/test/assets/folder")
            )
            for _ in range(10)
        ]

        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        assert all(len(r) == 1 for r in results)
        assert all(r[0]["id"] == "test_id" for r in results)
        assert call_count == 1
