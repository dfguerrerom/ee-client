import pytest

from eeclient.export.image import (
    DriveDestination,
    DriveOptions,
    ImageFileFormat,
)


def test_drive_options_accepts_legacy_geotiff_alias():
    opts = DriveOptions(
        file_format="GeoTIFF",
        drive_destination=DriveDestination(filename_prefix="out"),
    )
    assert opts.file_format == ImageFileFormat.GEO_TIFF


def test_drive_options_accepts_canonical_value():
    opts = DriveOptions(
        file_format="GEO_TIFF",
        drive_destination=DriveDestination(filename_prefix="out"),
    )
    assert opts.file_format == ImageFileFormat.GEO_TIFF


def test_drive_options_rejects_unknown_string():
    with pytest.raises(ValueError):
        DriveOptions(
            file_format="NotAFormat",
            drive_destination=DriveDestination(filename_prefix="out"),
        )
