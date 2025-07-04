from functools import wraps
import inspect
from .export import ExportProtocol  # noqa: F401
from .operations import OperationsProtocol  # noqa: F401
from .tasks import TasksProtocol  # noqa: F401


class _ModuleProxy:
    def __init__(self, session, module):
        self._session = session
        self._module = module

    def __repr__(self):
        return f"<Proxy for {self._module.__name__}>"


def expose_module_methods(proxy_cls, module):
    for name, func in inspect.getmembers(module, inspect.iscoroutinefunction):

        @wraps(func)
        async def method(self, *args, __func=func, **kwargs):
            return await __func(self._session, *args, **kwargs)

        method.__signature__ = inspect.signature(func)  # type: ignore
        setattr(proxy_cls, name, method)
