from typing import Protocol, Union, Optional, List
from pathlib import Path
from eeclient.models import MapTileOptions
from ee.image import Image
from ee.computedobject import ComputedObject


class OperationsProtocol(Protocol):
    async def get_map_id_async(
        self,
        ee_image: Image,
        vis_params: Union[dict, MapTileOptions] = {},
        bands: Optional[str] = None,
        format: Optional[str] = None,
    ) -> dict:
        ...

    async def get_info_async(
        self,
        ee_object: Union[ComputedObject, None] = None,
        workloadTag=None,
        serialized_object=None,
    ) -> dict:
        ...

    async def get_asset_async(
        self, asset_id: str, not_exists_ok: bool = True
    ) -> Optional[dict]:
        ...

    async def get_assets_async(self, folder: str = "") -> List[dict]:
        ...

    async def create_folder_async(self, folder: Union[Path, str]) -> str:
        ...

    async def delete_asset_async(self, asset_id: Union[str, Path]) -> None:
        ...

    async def delete_folder_async(
        self, folder_id: Union[str, Path], recursive: bool = False
    ) -> None:
        ...
