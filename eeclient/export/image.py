from enum import Enum
from typing import TYPE_CHECKING, Optional, Union
from pydantic import BaseModel, ConfigDict, model_validator, root_validator
from pydantic.alias_generators import to_camel

if TYPE_CHECKING:
    from eeclient.client import EESession

from ee import serializer
import ee


class ImageFileFormat(str, Enum):
    """Available file formats for image exports."""

    UNSPECIFIED = "IMAGE_FILE_FORMAT_UNSPECIFIED"
    JPEG = "JPEG"
    PNG = "PNG"
    AUTO_JPEG_PNG = "AUTO_JPEG_PNG"
    NPY = "NPY"
    GEO_TIFF = "GEO_TIFF"
    TF_RECORD_IMAGE = "TF_RECORD_IMAGE"
    ZIPPED_GEO_TIFF = "ZIPPED_GEO_TIFF"
    ZIPPED_GEO_TIFF_PER_BAND = "ZIPPED_GEO_TIFF_PER_BAND"


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
    file_format: ImageFileFormat
    drive_destination: DriveDestination

    # TODO: Add support for other export options
    # TODO: add support for format_options
    # See the api: https://developers.google.com/earth-engine/reference/rest/v1alpha/ImageFileExportOptions


class AssetOptions(BaseExportModel):
    earth_engine_destination: EarthEngineDestination


class GridDimensions(BaseModel):
    width: int
    height: int


class AffineTransform(BaseModel):
    scaleX: float
    shearX: float
    translateX: float
    shearY: float
    scaleY: float
    translateY: float


class PixelGrid(BaseModel):
    dimensions: Optional[GridDimensions] = None
    affine_transform: Optional[AffineTransform] = None
    crs_code: Optional[str] = None
    crs_wkt: Optional[str] = None

    @model_validator(mode="before")
    def check_crs_exclusivity(cls, values):
        crs_code = values.get("crs_code")
        crs_wkt = values.get("crs_wkt")
        if crs_code is not None and crs_wkt is not None:
            raise ValueError("Only one of crs_code or crs_wkt can be provided.")
        return values


class ExportOptions(BaseExportModel):
    # expression: dict
    description: str = "myExportTableTask"
    max_pixels: Optional[int] = None
    grid: Optional[PixelGrid] = None
    request_id: Optional[str] = None
    workload_tag: Optional[str] = None
    priority: Optional[int] = None

    file_export_options: Optional[DriveOptions] = None
    asset_export_options: Optional[AssetOptions] = None
    # TODO: Add support for other export options.
    # See the api: https://developers.google.com/earth-engine/reference/rest/v1alpha/projects.table/export#TableFileExportOptions


class ExtraExportOptions(BaseExportModel):
    scale: Optional[float] = None
    crs: Optional[str] = None
    crs_transform: Optional[AffineTransform] = None
    # TODO I didn't add dimensions...


async def export_image(
    client: "EESession",
    image,
    *,
    drive_options: Optional[DriveOptions] = None,
    asset_options: Optional[AssetOptions] = None,
    description: str = "myExportTableTask",
    max_pixels: Optional[int] = None,
    grid: Optional[PixelGrid] = None,
    request_id: Optional[str] = None,
    workload_tag: Optional[str] = None,
    priority: Optional[int] = None,
    region: Union[ee.Geometry, ee.Geometry.LinearRing, ee.Geometry.Polygon, str] = None,  # type: ignore
    scale: Optional[float] = None,
    crs: Optional[str] = None,
    crs_transform: Optional[AffineTransform] = None,
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

    export_options = ExportOptions(
        description=description,
        max_pixels=max_pixels,
        grid=grid,
        request_id=request_id,
        workload_tag=workload_tag,
        priority=priority,
        file_export_options=drive_options,
        asset_export_options=asset_options,
    )

    request_params = export_options.model_dump(by_alias=True, exclude_none=True)

    # These are additional params that are part of the earthengine-api
    image_options = ExtraExportOptions(
        scale=scale,
        crs=crs,
        crs_transform=crs_transform,
    )
    image_params = image_options.model_dump(by_alias=False, exclude_none=True)

    # Do this to avoid set the region in the ExportOptions model, it cannot validate it...
    # TODO: check the error
    image_params["region"] = region

    image, updated_image_params, dimensions_consumed = image._apply_crs_and_affine(
        image_params
    )
    image._apply_selection_and_scale(updated_image_params, dimensions_consumed)

    expression = serializer.encode(image, for_cloud_api=True)
    request_params["expression"] = expression

    url = "{earth_engine_api_url}/projects/{project}/image:export"
    return await client.rest_call("POST", url, data=request_params)


async def image_to_drive(
    client: "EESession",
    image,
    filename_prefix: str = "",
    folder: Optional[str] = None,
    file_format: ImageFileFormat = ImageFileFormat.GEO_TIFF,
    description: str = "Image",
    max_pixels: Optional[int] = None,
    grid: Optional[PixelGrid] = None,
    request_id: Optional[str] = None,
    workload_tag: Optional[str] = None,
    priority: Optional[int] = None,
    region: Union[ee.Geometry, ee.Geometry.LinearRing, ee.Geometry.Polygon, str] = None,  # type: ignore
    scale: Optional[float] = None,
    crs: Optional[str] = None,
    crs_transform: Optional[AffineTransform] = None,
) -> dict:
    """Abstracts the export of an image to Google Drive."""
    drive_options = DriveOptions(
        file_format=file_format,
        drive_destination=DriveDestination(
            filename_prefix=filename_prefix or description, folder=folder
        ),
    )

    return await export_image(
        client=client,
        image=image,
        drive_options=drive_options,
        description=description,
        max_pixels=max_pixels,
        grid=grid,
        request_id=request_id,
        workload_tag=workload_tag,
        priority=priority,
        region=region,
        scale=scale,
        crs=crs,
        crs_transform=crs_transform,
    )


async def image_to_asset(
    client: "EESession",
    image,
    asset_id: str,
    description: str = "myExportTableTask",
    max_pixels: Optional[int] = None,
    grid: Optional[PixelGrid] = None,
    request_id: Optional[str] = None,
    workload_tag: Optional[str] = None,
    priority: Optional[int] = None,
    region: Union[ee.Geometry, ee.Geometry.LinearRing, ee.Geometry.Polygon, str] = None,  # type: ignore
    scale: Optional[float] = None,
    crs: Optional[str] = None,
    crs_transform: Optional[AffineTransform] = None,
) -> dict:
    """Abstracts the export of an image to Earth Engine Asset."""
    asset_options = AssetOptions(
        earth_engine_destination=EarthEngineDestination(name=asset_id),
    )

    return await export_image(
        client=client,
        image=image,
        asset_options=asset_options,
        description=description,
        max_pixels=max_pixels,
        grid=grid,
        request_id=request_id,
        workload_tag=workload_tag,
        priority=priority,
        region=region,
        scale=scale,
        crs=crs,
        crs_transform=crs_transform,
    )
