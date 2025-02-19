from enum import Enum
from typing import TYPE_CHECKING, Optional
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

if TYPE_CHECKING:
    from eeclient.client import EESession

from ee import serializer, encodable


class TableFileFormat(str, Enum):
    """Available file formats for table exports."""

    UNSPECIFIED = "TABLE_FILE_FORMAT_UNSPECIFIED"
    CSV = "CSV"
    GEO_JSON = "GEO_JSON"
    KML = "KML"
    KMZ = "KMZ"
    SHP = "SHP"
    TF_RECORD_TABLE = "TF_RECORD_TABLE"


class BaseExportModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )


class DriveDestination(BaseExportModel):
    filename_prefix: str
    folder: Optional[str] = None


class EarthEngineDestination(BaseExportModel):
    name: str


class DriveOptions(BaseExportModel):
    # Removed default so that users must supply a file_format.
    file_format: TableFileFormat
    drive_destination: DriveDestination


class AssetOptions(BaseExportModel):
    earth_engine_destination: EarthEngineDestination


class ExportOptions(BaseExportModel):
    expression: dict
    description: str = "myExportTableTask"
    selectors: Optional[list] = None
    request_id: Optional[str] = None
    max_error_meters: Optional[int] = None
    max_vertices: Optional[int] = None
    workload_tag: Optional[str] = None
    priority: Optional[int] = None
    file_export_options: Optional[DriveOptions] = None
    drive_export_options: Optional[AssetOptions] = None
    # TODO: Add support for other export options.
    # See the api: https://developers.google.com/earth-engine/reference/rest/v1alpha/projects.table/export#TableFileExportOptions


async def export_table(
    client: "EESession",
    collection,
    *,
    drive_options: Optional[DriveOptions] = None,
    asset_options: Optional[AssetOptions] = None,
    description: str = "myExportTableTask",
    selectors: Optional[list] = None,
    request_id: Optional[str] = None,
    max_error_meters: Optional[int] = None,
    max_vertices: Optional[int] = None,
    workload_tag: Optional[str] = None,
    priority: Optional[int] = None,
) -> dict:
    """
    Export a table to either Google Drive or Earth Engine Asset.

    Exactly one of drive_options or asset_options must be provided.
    """
    if (drive_options is None and asset_options is None) or (
        drive_options is not None and asset_options is not None
    ):
        raise ValueError(
            "You must provide exactly one of drive_options or asset_options."
        )

    if isinstance(collection, encodable.Encodable):
        expression = serializer.encode(collection, for_cloud_api=True)

    export_options = ExportOptions(
        expression=expression,  # type: ignore
        description=description,
        selectors=selectors,
        request_id=request_id,
        max_error_meters=max_error_meters,
        max_vertices=max_vertices,
        workload_tag=workload_tag,
        priority=priority,
        file_export_options=drive_options,
        drive_export_options=asset_options,
    )

    params = export_options.model_dump(by_alias=True, exclude_none=True)

    url = "{earth_engine_api_url}/projects/{project}/table:export"
    return await client.rest_call("POST", url, data=params)


async def table_to_drive(
    client: "EESession",
    collection,
    filename_prefix: str,
    file_format: TableFileFormat,
    folder: Optional[str] = None,
    description: str = "myExportTableTask",
    selectors: Optional[list] = None,
    max_vertices: Optional[int] = None,
    priority: Optional[int] = None,
) -> dict:
    """
    Export a table to Google Drive.
    """
    drive_destination = DriveDestination(filename_prefix=filename_prefix, folder=folder)
    drive_options = DriveOptions(
        file_format=file_format, drive_destination=drive_destination
    )

    return await export_table(
        client=client,
        collection=collection,
        drive_options=drive_options,
        description=description,
        selectors=selectors,
        max_vertices=max_vertices,
        priority=priority,
    )


async def table_to_asset(
    client: "EESession",
    collection,
    asset_id: str,
    description: str = "myExportTableTask",
    selectors: Optional[list] = None,
    max_vertices: Optional[int] = None,
    priority: Optional[int] = None,
) -> dict:
    """
    Export a table to Earth Engine Asset.
    """
    asset_options = AssetOptions(
        earth_engine_destination=EarthEngineDestination(name=asset_id)
    )

    return await export_table(
        client=client,
        collection=collection,
        asset_options=asset_options,
        description=description,
        selectors=selectors,
        max_vertices=max_vertices,
        priority=priority,
    )
