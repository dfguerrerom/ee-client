from typing import Protocol, Optional
from eeclient.tasks import TasksResponse, Task


class TasksProtocol(Protocol):
    async def get_tasks_async(self) -> TasksResponse:
        ...

    async def get_task_async(self, task_id: str) -> Optional[Task]:
        ...

    async def get_task_by_name_async(self, asset_name: str) -> Optional[Task]:
        ...
