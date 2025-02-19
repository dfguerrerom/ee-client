from typing import TYPE_CHECKING, List, Optional, Union

from eeclient.exceptions import EERestException
from eeclient.logger import logger

if TYPE_CHECKING:
    from eeclient.async_client import AsyncEESession

from pydantic import BaseModel, Field
from typing import List, Optional, Any
from datetime import datetime


class CamelCaseModel(BaseModel):
    model_config = {
        "populate_by_name": True,
        "alias_generator": lambda field_name: "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(field_name.split("_"))
        ),
    }


class Stage(CamelCaseModel):
    display_name: str
    complete_work_units: int
    total_work_units: str
    description: str


class TaskMetadata(CamelCaseModel):
    type_info: str = Field(alias="@type")
    state: str
    description: str
    priority: int
    create_time: datetime
    update_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    type: str
    destination_uris: Optional[List[str]] = None
    attempt: int
    progress: Optional[float] = None
    stages: Optional[List[Stage]] = None
    batch_eecu_usage_seconds: Optional[float] = None


class Response(CamelCaseModel):
    type_info: str = Field(alias="@type")


class Task(CamelCaseModel):
    name: str
    metadata: TaskMetadata
    done: Optional[bool] = None
    response: Optional[Response] = None
    error: Optional[Any] = (
        None  # Add error field which might be present instead of response
    )


class TasksResponse(CamelCaseModel):
    operations: List[Task]


async def get_tasks(async_client: "AsyncEESession") -> TasksResponse:
    """Search for the described task in the user Task list return None if nothing is found.

    Args:
        task_descripsion: the task description

    Returns:
        return the found task else None
    """
    url = "{earth_engine_api_url}/projects/{project}" + "/operations"
    response_data = await async_client.rest_call("GET", url)
    operations_response = TasksResponse.model_validate(response_data)
    return operations_response


async def get_task(async_client: "AsyncEESession", task_id: str):
    """Search for the described task in the user Task list return None if nothing is found.

    Args:
        task_descripsion: the task description

    Returns:
        return the found task else None
    """
    url = "{earth_engine_api_url}/projects/{project}" + "/operations/" + task_id
    try:
        response_data = await async_client.rest_call("GET", url)
        task = Task.model_validate(response_data)
        return task
    except EERestException as e:
        if e.code == 404:
            logger.info(f"Task {task_id} not found")
            return None
    except Exception as e:
        logger.error(f"Error in get_task: {e}")
        raise e


async def get_task_by_name(
    async_client: "AsyncEESession", asset_name: str
) -> Optional[Task]:
    """Search for the described task in the user Task list return None if nothing is found.

    Args:
        task_descripsion: the task description

    Returns:
        return the found task else None
    """
    operations_response = await get_tasks(async_client)
    for task in operations_response.operations:
        if task.metadata.description == asset_name:
            return task
    return None
