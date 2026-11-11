"""Tests for the Task / Stage models in eeclient.tasks."""

import pytest

from eeclient.tasks import Stage, Task


def test_stage_allows_missing_work_units():
    """The Cloud API omits completeWorkUnits/totalWorkUnits on a stage until
    that stage reports progress; the model must accept this."""
    stage = Stage.model_validate(
        {
            "displayName": "Create List of Assets",
            "description": "Listing of temporary files.",
        }
    )
    assert stage.display_name == "Create List of Assets"
    assert stage.complete_work_units is None
    assert stage.total_work_units is None


def test_stage_allows_missing_description():
    """Some stages omit `description` entirely before reporting progress."""
    stage = Stage.model_validate({"displayName": "Write Files"})
    assert stage.display_name == "Write Files"
    assert stage.description is None
    assert stage.complete_work_units is None
    assert stage.total_work_units is None


def test_task_validates_with_incomplete_stages():
    """Regression for issue #22: polling a freshly-RUNNING export returns
    stages without completeWorkUnits / totalWorkUnits and must not raise."""
    payload = {
        "name": "projects/ee-project/operations/ABC123",
        "metadata": {
            "@type": "type.googleapis.com/google.earthengine.v1.OperationMetadata",
            "state": "RUNNING",
            "description": "my-export",
            "priority": 100,
            "createTime": "2026-05-13T12:00:00Z",
            "type": "EXPORT_IMAGE",
            "stages": [
                {
                    "displayName": "Create List of Assets",
                    "description": "Listing of temporary files.",
                },
                {
                    "displayName": "Write Files",
                    "description": "Writing files to the export destination.",
                },
            ],
        },
        "done": False,
    }

    task = Task.model_validate(payload)
    assert task.id == "ABC123"
    assert task.metadata.stages is not None
    assert len(task.metadata.stages) == 2
    assert task.metadata.stages[0].complete_work_units is None
    assert task.metadata.stages[0].total_work_units is None


@pytest.mark.parametrize("work_units", [2.1129568106312298, 0, 3, None])
def test_task_preserves_stage_work_units(work_units):
    """Export task progress may be fractional, integral, or not yet reported."""
    task = Task.model_validate(
        {
            "name": "projects/ee-project/operations/ABC123",
            "metadata": {
                "@type": "type.googleapis.com/google.earthengine.v1.OperationMetadata",
                "state": "RUNNING",
                "description": "forest-change-mask-export",
                "priority": 100,
                "createTime": "2026-10-20T12:00:00Z",
                "type": "EXPORT_IMAGE",
                "stages": [
                    {
                        "displayName": "Write Files",
                        "completeWorkUnits": work_units,
                        "totalWorkUnits": "10",
                    },
                ],
            },
            "done": False,
        }
    )

    assert task.metadata.stages[0].complete_work_units == work_units
    assert (
        task.model_dump(by_alias=True)["metadata"]["stages"][0]["completeWorkUnits"]
        == work_units
    )


@pytest.mark.asyncio
async def test_cancel_task_posts_to_cancel_url():
    from unittest.mock import AsyncMock, MagicMock

    from eeclient.tasks import cancel_task_async

    client = MagicMock()
    client.rest_call = AsyncMock(return_value={})

    assert await cancel_task_async(client, "ABC123") is None

    client.rest_call.assert_awaited_once()
    args, kwargs = client.rest_call.call_args
    assert args[0] == "POST"
    assert args[1] == (
        "{earth_engine_api_url}/projects/{project}/operations/ABC123:cancel"
    )


@pytest.mark.asyncio
async def test_cancel_task_propagates_rest_errors():
    from unittest.mock import AsyncMock, MagicMock

    from eeclient.exceptions import EERestException
    from eeclient.tasks import cancel_task_async

    client = MagicMock()
    error = EERestException({"code": 400, "message": "already done"})
    client.rest_call = AsyncMock(side_effect=error)

    with pytest.raises(EERestException):
        await cancel_task_async(client, "ABC123")
