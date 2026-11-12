"""Build and publish the NOAA GFS 10 m wind layer from ArcGIS Pro Python."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from math import floor, isclose
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4


# Used when the file is run as a script or as an ArcGIS Pro script tool.
OUTPUT_DIR = r"C:\GIS\Wind"
FEDERATED_SERVER = ""  # Blank prepares the raster only. Use the server URL, e.g. https://gis.example.com/server.
SERVICE_NAME = "Global_Wind_10m"
SERVER_FOLDER = ""  # Blank publishes at the ArcGIS Server root.
PORTAL_FOLDER = ""
OVERWRITE = False  # True when updating the same service after the first publish.
SHARING_LEVEL = "OWNER"  # OWNER, ORGANIZATION or EVERYONE.
SHARING_GROUPS = ""  # Comma-separated Portal group names, if needed.


NOMADS = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
GFS_ARCHIVE = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
GRID_SIZE = [1440, 721]
GRID_TRANSFORM = (-180.125, 0.25, 0.0, 90.125, 0.0, -0.25)
WIND_NODATA = -9999.0


class WindInputError(ValueError):
    """The source did not provide a usable global 10 m wind GRIB2."""


def cycle_candidates(explicit_cycle):
    if explicit_cycle:
        if len(explicit_cycle) != 10 or not explicit_cycle.isascii() or not explicit_cycle.isdigit():
            raise ValueError("Enter the GFS cycle as YYYYMMDDHH in UTC.")
        cycle = datetime.strptime(explicit_cycle, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        if cycle.hour not in (0, 6, 12, 18):
            raise ValueError("The GFS cycle hour must be 00, 06, 12 or 18 UTC.")
        return [cycle]

    now = datetime.now(timezone.utc)
    latest = now.replace(hour=(now.hour // 6) * 6, minute=0, second=0, microsecond=0)
    return [latest - timedelta(hours=6 * offset) for offset in range(5)]


def download_grib(cycle, destination):
    params = {
        "dir": cycle.strftime("/gfs.%Y%m%d/%H/atmos"),
        "file": cycle.strftime("gfs.t%Hz.pgrb2.0p25.f000"),
        "lev_10_m_above_ground": "on",
        "var_UGRD": "on",
        "var_VGRD": "on",
    }
    request = Request(f"{NOMADS}?{urlencode(params)}", headers={"User-Agent": "ArcGIS-GFS-Wind/1.0"})
    with urlopen(request, timeout=60) as response, destination.open("wb") as output:
        signature = response.read(4)
        if signature != b"GRIB":
            raise WindInputError("NOMADS returned a response without a GRIB signature.")
        output.write(signature)
        shutil.copyfileobj(response, output)


def archive_ranges(index_text):
    offsets = set()
    matches = {"UGRD": set(), "VGRD": set()}
    for line in index_text.splitlines():
        fields = line.split(":", 5)
        if len(fields) < 5 or not fields[1].isdigit():
            continue
        offset = int(fields[1])
        offsets.add(offset)
        variable, level = fields[3].strip(), fields[4].strip()
        if level == "10 m above ground" and variable in matches:
            matches[variable].add(offset)

    if any(len(found) != 1 for found in matches.values()):
        raise WindInputError("The NOAA archive index must contain one 10 m UGRD and one VGRD record.")
    ordered = sorted(offsets)
    next_offsets = dict(zip(ordered, ordered[1:]))
    for start in sorted(matches["UGRD"] | matches["VGRD"]):
        yield start, next_offsets.get(start)


def stream_bytes(response, size, output=None):
    while size:
        chunk = response.read(min(size, 1024 * 1024))
        if not chunk:
            raise WindInputError("The NOAA archive response ended before all requested bytes arrived.")
        if output is not None:
            output.write(chunk)
        size -= len(chunk)


def write_grib_record(response, output, header, expected_size):
    if len(header) != 16 or header[:4] != b"GRIB" or header[7] != 2:
        raise WindInputError("The NOAA archive did not contain a GRIB2 record at an indexed offset.")
    size = int.from_bytes(header[8:16], "big")
    if size < 20 or (expected_size is not None and size != expected_size):
        raise WindInputError("A NOAA GRIB2 record length did not match the archive index.")

    output.write(header)
    stream_bytes(response, size - 20, output)
    trailer = response.read(4)
    if trailer != b"7777":
        raise WindInputError("A NOAA GRIB2 record was incomplete or missing its end marker.")
    output.write(trailer)
    return size


def extract_full_archive(response, output, ranges):
    first_header = response.read(16)
    if len(first_header) != 16 or first_header[:4] != b"GRIB" or first_header[7] != 2:
        raise WindInputError("The response body did not start with a GRIB2 file.")

    position = len(first_header)
    for start, next_start in ranges:
        if start == 0:
            header = first_header
        else:
            if start < position:
                raise WindInputError("The NOAA archive index points inside a previous GRIB2 record.")
            stream_bytes(response, start - position)
            header = response.read(16)
        expected_size = next_start - start if next_start is not None else None
        position = start + write_grib_record(response, output, header, expected_size)


def download_archive_grib(cycle, destination):
    key = cycle.strftime("gfs.%Y%m%d/%H/atmos/gfs.t%Hz.pgrb2.0p25.f000")
    url = f"{GFS_ARCHIVE}/{key}"
    with urlopen(url + ".idx", timeout=60) as response:
        ranges = list(archive_ranges(response.read().decode("ascii")))

    with destination.open("wb") as output:
        for start, next_start in ranges:
            end = next_start - 1 if next_start is not None else None
            range_end = str(end) if end is not None else ""
            request = Request(url, headers={"Range": f"bytes={start}-{range_end}", "Accept-Encoding": "identity"})
            with urlopen(request, timeout=60) as response:
                content_range = response.headers.get("Content-Range", "")
                if response.status == 200 and not content_range:
                    # Start over if an earlier partial response already supplied one record.
                    output.seek(0)
                    output.truncate()
                    try:
                        extract_full_archive(response, output, ranges)
                    except WindInputError as error:
                        raise WindInputError(f"The NOAA archive returned HTTP 200 to a Range request. {error}") from error
                    return
                if response.status not in (200, 206):
                    raise WindInputError(f"Unexpected HTTP {response.status} from the NOAA archive for a Range request.")

                returned = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
                if returned is None:
                    raise WindInputError("The NOAA archive response is missing a valid Content-Range header.")
                actual_start, actual_end, total = map(int, returned.groups())
                if (actual_start != start or actual_end != (end if end is not None else total - 1)
                        or not actual_start <= actual_end < total):
                    raise WindInputError("The NOAA archive returned a different byte range than requested.")
                write_grib_record(response, output, response.read(16), actual_end - actual_start + 1)


def grib_header_size(path):
    """Read the dimensions from the first GRIB2 grid definition for diagnostics."""
    with path.open("rb") as stream:
        header = stream.read(16)
        if len(header) != 16 or header[:4] != b"GRIB" or header[7] != 2:
            return None
        message_size = int.from_bytes(header[8:16], "big")
        while stream.tell() + 5 < message_size:
            section_start = stream.tell()
            prefix = stream.read(5)
            if len(prefix) != 5:
                break
            length = int.from_bytes(prefix[:4], "big")
            if length < 5 or section_start + length > message_size:
                break
            if prefix[4] == 3 and length >= 38:
                definition = prefix + stream.read(33)
                if len(definition) == 38 and definition[12:14] == b"\x00\x00":
                    return [int.from_bytes(definition[offset:offset + 4], "big") for offset in (30, 34)]
                break
            stream.seek(section_start + length)
    return None


def validate_exported_grid(size, transform):
    if size not in (GRID_SIZE, [GRID_SIZE[0], GRID_SIZE[1] - 1]):
        raise ValueError(f"Expected a global 1440 x 720 or 1440 x 721 raster, got {size}.")
    if transform is None or len(transform) != 6:
        raise ValueError("The raster has no geographic grid transform.")

    width, height = size
    _, dx, rx, north, ry, dy = transform
    south = north + height * dy
    # ArcGIS can clip the polar edge cells when it copies the GRIB2 layer.
    polar_tolerance = 0.125 + 1e-5
    if not (isclose(dx * width, 360, abs_tol=1e-4)
            and isclose(dy, -0.25, abs_tol=0.25 / height + 1e-5)
            and isclose(rx, 0, abs_tol=1e-10) and isclose(ry, 0, abs_tol=1e-10)
            and isclose(north, 90, abs_tol=polar_tolerance)
            and isclose(south, -90, abs_tol=polar_tolerance)):
        raise ValueError(f"Raster {size} does not cover the global quarter-degree grid: {transform}.")


def wind_bands(info, cycle, *, exported=False):
    size = info.get("size")
    transform = info.get("geoTransform", ())
    if exported:
        validate_exported_grid(size, transform)
        if not isclose(transform[0], -180.0, abs_tol=0.125 + 1e-5):
            raise ValueError(f"Unexpected western raster edge after longitude normalisation: {transform[0]}Â°.")
    else:
        if size != GRID_SIZE:
            raise ValueError(f"Expected the global 1440 x 721 grid, got {size}.")
        if len(transform) != 6 or not all(isclose(a, b, abs_tol=1e-5) for a, b in zip(transform, GRID_TRANSFORM)):
            raise ValueError(f"Unexpected GFS grid after longitude normalisation: {transform}")

    selected = {}
    timestamp = int(cycle.timestamp())
    for band in info.get("bands", []):
        metadata = band.get("metadata", {}).get("", {})
        component = metadata.get("GRIB_ELEMENT")
        if component not in ("UGRD", "VGRD") or metadata.get("GRIB_SHORT_NAME") != "10-HTGL":
            continue
        for field in ("GRIB_REF_TIME", "GRIB_VALID_TIME"):
            if metadata.get(field, "").strip().split()[:1] != [str(timestamp)]:
                raise ValueError(f"Unexpected {field} for {component} in cycle {cycle.isoformat()}.")
        if component in selected:
            raise ValueError(f"Found more than one {component} band for the selected cycle.")
        selected[component] = band["band"]

    if set(selected) != {"UGRD", "VGRD"}:
        raise ValueError("Expected both 10 m UGRD and VGRD bands in the GRIB metadata.")
    return selected["UGRD"], selected["VGRD"]


def read_raster_info(gdal, dataset):
    bands = []
    for index in range(1, dataset.RasterCount + 1):
        band = dataset.GetRasterBand(index)
        bands.append({
            "band": index,
            "type": gdal.GetDataTypeName(band.DataType),
            "metadata": {"": band.GetMetadata()},
        })
    return {
        "size": [dataset.RasterXSize, dataset.RasterYSize],
        "geoTransform": dataset.GetGeoTransform(can_return_null=True) or (),
        "bands": bands,
    }


def grib_sections(message):
    offset = 0
    while offset < len(message):
        length = int.from_bytes(message[offset:offset + 4], "big")
        if length < 5 or offset + length > len(message):
            raise WindInputError("Invalid GRIB2 section length in the downloaded file.")
        section = message[offset:offset + length]
        yield section[4], section
        offset += length


def native_wind_messages(path, cycle):
    """Extract standalone U/V fields for the ArcGIS reader, including shared-grid messages."""
    wind = {}
    with path.open("rb") as stream:
        while header := stream.read(16):
            size = int.from_bytes(header[8:16], "big")
            if len(header) != 16 or header[:4] != b"GRIB" or header[7] != 2 or size < 20:
                raise WindInputError(f"{path.name} does not contain complete GRIB2 messages.")
            message = stream.read(size - 16)
            if len(message) != size - 16 or message[-4:] != b"7777":
                raise WindInputError(f"A GRIB2 message in {path.name} is incomplete.")
            if header[6] != 0:  # Meteorological discipline.
                continue

            shared, field = {}, {}
            previous_bitmap = None
            for number, section in grib_sections(message[:-4]):
                if number <= 3:
                    shared[number] = section
                    continue
                field[number] = section
                if number == 6 and len(section) > 5 and section[5] not in (254, 255):
                    previous_bitmap = section
                if number != 7:
                    continue

                product = field.get(4, b"")
                identification = shared.get(1, b"")
                grid = shared.get(3, b"")
                if len(product) >= 34 and len(identification) >= 19 and len(grid) >= 38:
                    parameter = product[10]
                    height_scale = product[23]
                    if height_scale & 128:
                        height_scale = -(height_scale & 127)
                    height = int.from_bytes(product[24:28], "big") * 10.0 ** (-height_scale)
                    reference = (int.from_bytes(identification[12:14], "big"), *identification[14:19])
                    matches_cycle = reference == (cycle.year, cycle.month, cycle.day, cycle.hour, 0, 0)
                    if (product[7:9] == b"\0\0" and product[9] == 2 and parameter in (2, 3)
                            and product[18:22] == b"\0\0\0\0" and product[22] == 103
                            and height == 10 and matches_cycle):
                        component = "UGRD" if parameter == 2 else "VGRD"
                        shape = [int.from_bytes(grid[i:i + 4], "big") for i in (30, 34)]
                        if grid[12:14] != b"\0\0" or shape != GRID_SIZE:
                            raise WindInputError(f"{component} has GRIB2 grid {shape}; expected the global 0.25Â° grid.")
                        if component in wind:
                            raise WindInputError(f"Found more than one 10 m {component} field for this model cycle.")
                        if not all(key in field for key in (5, 6, 7)) or len(field[6]) < 6:
                            raise WindInputError(f"The GRIB2 sections for {component} are incomplete.")
                        bitmap = field[6] if field[6][5] != 254 else previous_bitmap
                        if bitmap is None:
                            raise WindInputError(f"The shared GRIB2 bitmap for {component} is missing.")
                        body = b"".join((identification, shared.get(2, b""), grid,
                                         product, field[5], bitmap, field[7], b"7777"))
                        wind[component] = header[:8] + (16 + len(body)).to_bytes(8, "big") + body
                field.clear()

    if set(wind) != {"UGRD", "VGRD"}:
        raise WindInputError(f"{path.name} lacks a 10 m above-ground U/V analysis pair for {cycle.isoformat()}.")
    return wind


@contextmanager
def temporary_wind_stage(arcpy, parent, prefix):
    temporary = TemporaryDirectory(prefix=prefix, dir=parent, ignore_cleanup_errors=True)
    folder = Path(temporary.name)
    try:
        yield folder
    finally:
        temporary.cleanup()
        if folder.exists():
            try:
                arcpy.management.ClearWorkspaceCache(str(folder))
            except (arcpy.ExecuteError, RuntimeError, OSError) as error:
                arcpy.AddWarning(f"Could not clear the ArcGIS workspace cache for {folder}: {error}")
            temporary.cleanup()
            if folder.exists():
                arcpy.AddWarning(f"ArcGIS could not remove temporary files in {folder}. They can be removed after Pro closes.")


def arcpy_wind_array(arcpy, gdal, np, path, component):
    tiff = path.with_suffix(".tif")
    layer = arcpy.md.MakeMultidimensionalRasterLayer(
        str(path), f"wind_{component.lower()}_{uuid4().hex}", dimensionless="NO_DIMENSIONS",
    ).getOutput(0)
    try:
        arcpy.management.CopyRaster(layer, str(tiff), pixel_type="32_BIT_FLOAT", format="TIFF")
    finally:
        try:
            arcpy.management.Delete(layer)
        except (arcpy.ExecuteError, RuntimeError, TypeError, OSError) as error:
            arcpy.AddWarning(f"Could not delete the temporary ArcGIS layer for {component}: {error}")
        finally:
            del layer

    with gdal.OpenEx(str(tiff), gdal.OF_RASTER, allowed_drivers=["GTiff"]) as raster:
        shape = [raster.RasterXSize, raster.RasterYSize]
        if raster.RasterCount != 1:
            raise WindInputError(f"ArcGIS copied {component} with {raster.RasterCount} bands; expected one.")
        transform = raster.GetGeoTransform(can_return_null=True)
        try:
            validate_exported_grid(shape, transform)
        except ValueError as error:
            raise WindInputError(f"ArcGIS copy of {component}: {error}") from error
        width, height = shape
        x0, dx, rx, y0, ry, dy = transform
        band = raster.GetRasterBand(1)
        try:
            nodata = band.GetNoDataValue()
            samples = band.ReadRaster(0, 0, width, height, buf_type=gdal.GDT_Float32)
        finally:
            del band

    values = np.frombuffer(samples, dtype=np.float32).reshape(height, width)
    invalid = ~np.isfinite(values)
    if nodata is not None:
        invalid |= values == np.float32(nodata)
    if invalid.any():
        values = np.where(invalid, np.float32(WIND_NODATA), values)
    # Move the seam by whole columns and keep the ArcGIS latitude coordinates.
    shift = floor((x0 + 180.0) / dx + 0.5 + 1e-8)
    normalized = (x0 - shift * dx, dx, rx, y0 - shift * ry, ry, dy)
    return np.roll(values, shift % width, axis=1), normalized


def convert_grib_with_arcpy(gdal, grib, output, cycle):
    import arcpy
    import numpy as np
    from osgeo import osr

    fields = native_wind_messages(grib, cycle)
    with temporary_wind_stage(arcpy, output.parent, "wind-native-") as folder:
        with arcpy.EnvManager(addOutputsToMap=False):
            arrays = []
            for component in ("UGRD", "VGRD"):
                path = folder / f"{component}.grb2"
                path.write_bytes(fields[component])
                arrays.append(arcpy_wind_array(arcpy, gdal, np, path, component))

    (u_values, transform), (v_values, v_transform) = arrays
    if u_values.shape != v_values.shape or not all(
        isclose(u, v, abs_tol=1e-8) for u, v in zip(transform, v_transform)
    ):
        raise WindInputError(
            f"ArcGIS produced different U/V grids: U {u_values.shape} {transform}, "
            f"V {v_values.shape} {v_transform}."
        )

    height, width = u_values.shape
    timestamp = str(int(cycle.timestamp()))
    with gdal.GetDriverByName("GTiff").Create(
        str(output), width, height, 2, gdal.GDT_Float32, options=["COMPRESS=DEFLATE"],
    ) as raster:
        raster.SetGeoTransform(transform)
        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromEPSG(4326)
        raster.SetSpatialRef(spatial_reference)
        for index, (component, values) in enumerate((("UGRD", u_values), ("VGRD", v_values)), start=1):
            band = raster.GetRasterBand(index)
            band.SetNoDataValue(WIND_NODATA)
            band.SetMetadata({"GRIB_ELEMENT": component, "GRIB_SHORT_NAME": "10-HTGL",
                              "GRIB_REF_TIME": timestamp, "GRIB_VALID_TIME": timestamp})
            band.WriteRaster(0, 0, width, height, values.tobytes(), buf_type=gdal.GDT_Float32)
            del band


def verify_tiff(gdal, path, cycle):
    try:
        with gdal.OpenEx(str(path), gdal.OF_RASTER, allowed_drivers=["GTiff"]) as dataset:
            info = read_raster_info(gdal, dataset)
        types = [band["type"] for band in info["bands"]]
        if wind_bands(info, cycle, exported=True) != (1, 2) or types != ["Float32", "Float32"]:
            raise ValueError("Expected two Float32 bands ordered U, V.")
    except (RuntimeError, ValueError) as error:
        raise ValueError(f"GeoTIFF {path}: {error}") from error


def convert_grib(gdal, grib, output, cycle):
    with gdal.ExceptionMgr(), gdal.config_option("GRIB_ADJUST_LONGITUDE_RANGE", "YES"):
        input_valid = False
        try:
            with gdal.OpenEx(str(grib), gdal.OF_RASTER, allowed_drivers=["GRIB"]) as source:
                u_band, v_band = wind_bands(read_raster_info(gdal, source), cycle)
                input_valid = True
                with gdal.Translate(
                    str(output), source, format="GTiff",
                    bandList=[u_band, v_band], outputType=gdal.GDT_Float32,
                    outputSRS="EPSG:4326", creationOptions=["COMPRESS=DEFLATE"],
                ):
                    pass  # Close the output before checking it or passing it to ArcPy.
        except (RuntimeError, ValueError) as error:
            if input_valid:
                raise
            header_size = grib_header_size(grib) if grib.is_file() else None
            detail = f"Input GRIB2 {grib.name} (raw grid: {header_size}): {error}"
            if header_size != GRID_SIZE:
                raise WindInputError(detail) from error
            import arcpy
            arcpy.AddWarning(f"{detail} Trying the ArcGIS Pro GRIB reader.")
            try:
                convert_grib_with_arcpy(gdal, grib, output, cycle)
            except (arcpy.ExecuteError, RuntimeError, TypeError, ValueError, OSError) as native_error:
                raise WindInputError(f"{detail} ArcGIS Pro reader: {native_error}") from native_error
        verify_tiff(gdal, output, cycle)


def fetch_global_wind(arcpy, gdal, cycle, grib, output):
    try:
        download_grib(cycle, grib)
        convert_grib(gdal, grib, output, cycle)
        return
    except (WindInputError, URLError, TimeoutError) as error:
        if isinstance(error, HTTPError) and error.code == 404:
            raise
        arcpy.AddWarning(f"NOMADS {cycle.isoformat()}: {error} Trying NOAA's Open Data archive.")

    archive_grib = grib.with_stem(f"{grib.stem}_archive")
    download_archive_grib(cycle, archive_grib)
    convert_grib(gdal, archive_grib, output, cycle)


def prepare_wind(output_dir, *, cycle=None, grib_file=None):
    """Download a recent GFS analysis or convert a local GRIB2. Return path and UTC time."""
    import arcpy
    try:
        from osgeo import gdal
    except ImportError as error:
        raise RuntimeError(
            "The GDAL Python bindings are unavailable in this environment. "
            "Run this in ArcGIS Pro's Python environment. ArcGIS Pro 3.5 includes GDAL."
        ) from error

    if grib_file and not cycle:
        raise ValueError("Supply cycle='YYYYMMDDHH' when using a local GRIB2.")
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cycles = cycle_candidates(cycle)

    for index, model_time in enumerate(cycles):
        name = model_time.strftime("wind_10m_%Y%m%d%HZ.tif")
        output = output_dir / name
        if output.exists():
            try:
                with gdal.ExceptionMgr():
                    verify_tiff(gdal, output, model_time)
            except ValueError as error:
                output = output.with_stem(output.stem + "_global")
                arcpy.AddWarning(f"{error} Using {output.name} for the replacement raster.")
            else:
                if not grib_file:
                    return output, model_time
            if output.exists() and not grib_file:
                with gdal.ExceptionMgr():
                    verify_tiff(gdal, output, model_time)
                return output, model_time

        with temporary_wind_stage(arcpy, output_dir, "gfs-stage-") as stage:
            grib = Path(grib_file) if grib_file else stage / "wind.grib2"
            staged_tif = stage / output.name
            if grib_file:
                convert_grib(gdal, grib, staged_tif, model_time)
            else:
                arcpy.AddMessage(f"Checking NOAA GFS {model_time.isoformat()}")
                try:
                    fetch_global_wind(arcpy, gdal, model_time, grib, staged_tif)
                except HTTPError as error:
                    if error.code != 404 or index == len(cycles) - 1:
                        raise
                    sleep(10)
                    continue

            if output.exists():
                with gdal.ExceptionMgr():
                    verify_tiff(gdal, output, model_time)
                return output, model_time
            arcpy.management.SetRasterProperties(str(staged_tif), data_type="VECTOR_UV")
            # Keep the TIFF sidecars that ArcGIS uses to store raster metadata.
            for sidecar in stage.glob(staged_tif.stem + ".*"):
                if sidecar != staged_tif:
                    sidecar.replace(output_dir / sidecar.name)
            staged_tif.replace(output)
            return output, model_time


def publish_wind(raster, valid_time, *, server_url, service_name=SERVICE_NAME,
                 server_folder=SERVER_FOLDER, portal_folder=PORTAL_FOLDER,
                 overwrite=False, sharing_level=SHARING_LEVEL, sharing_groups=SHARING_GROUPS):
    """Publish or overwrite a dynamic imagery layer using the active Pro Portal sign-in."""
    import arcpy

    server_url = server_url.strip().rstrip("/")
    if not server_url.startswith("https://") or "/rest/" in server_url:
        raise ValueError("Use the HTTPS federated Server URL, without /rest/services or /ImageServer.")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,120}", service_name):
        raise ValueError("Use a service name containing only letters, digits and underscores.")
    if sharing_level not in {"OWNER", "ORGANIZATION", "EVERYONE"}:
        raise ValueError("Use OWNER, ORGANIZATION or EVERYONE for sharing_level.")
    if not arcpy.GetSigninToken():
        raise RuntimeError("Sign in to the target Enterprise Portal in ArcGIS Pro before publishing.")

    draft = arcpy.sharing.CreateSharingDraft(
        server_type="FEDERATED_SERVER",
        service_type="WEB_IMAGERY_LAYER",
        service_name=service_name,
        draft_value=str(raster),
    )
    draft.targetServer = server_url
    draft.serverFolder = server_folder
    draft.portalFolder = portal_folder
    draft.copyDataToServer = False  # Register the output folder to use referenced data.
    draft.overwriteExistingService = overwrite
    draft.cacheControlMaxAge = 300
    draft.summary = "NOAA GFS 0.25-degree global 10 m wind analysis"
    draft.description = f"NOAA GFS global 10 m wind. Model valid time: {valid_time.isoformat()}."
    draft.credits = "NOAA / NCEP Global Forecast System"
    draft.tags = "wind, weather, NOAA, GFS"
    draft.sharing.sharingLevel = sharing_level
    draft.sharing.groups = sharing_groups

    with TemporaryDirectory(prefix="wind-publish-") as temp_dir:
        draft_path = str(Path(temp_dir) / f"{service_name}.sddraft")
        definition_path = str(Path(temp_dir) / f"{service_name}.sd")
        draft.exportToSDDraft(draft_path)
        arcpy.server.StageService(draft_path, definition_path)
        result = arcpy.server.UploadServiceDefinition(definition_path, server_url)
        return result.getOutput(1)  # The second derived output is the REST service URL.


def add_flow_to_web_map(web_map_id, service_url, title="Global Wind at 10 m"):
    """Add or style the imagery layer in an existing web map using the active Portal."""
    from arcgis.gis import GIS

    portal = GIS("home")
    item = portal.content.get(web_map_id)
    if item is None or item.type != "Web Map":
        raise ValueError("Supply the item ID of an editable Portal web map.")
    data = item.get_data()
    if not isinstance(data, dict):
        raise ValueError("Could not read the web map data.")

    service_url = service_url.rstrip("/")
    layers = data.setdefault("operationalLayers", [])
    layer = next(
        (entry for entry in layers if entry.get("url", "").rstrip("/") == service_url),
        None,
    )
    if layer is None:
        layer = {
            "id": "global-wind-" + uuid4().hex,
            "title": title,
            "url": service_url,
            "layerType": "ArcGISImageServiceLayer",
            "visibility": True,
        }
        layers.append(layer)
    drawing_info = layer.setdefault("layerDefinition", {}).setdefault("drawingInfo", {})
    drawing_info["renderer"] = {
        "type": "flowRenderer",
        "flowRepresentation": "flow_to",
        "color": [63, 201, 244, 255],
        "density": 0.8,
        "flowSpeed": 10,
        "trailLength": 100,
        "maxPathLength": 200,
        "trailWidth": 1,
        "authoringInfo": {"type": "flow", "flowTheme": "flow-line"},
    }
    if not item.update(data=json.dumps(data)):
        raise RuntimeError("The Portal rejected the web map update.")
    return item


def run():
    """Entry point for an ArcGIS Pro script tool."""
    import arcpy

    raster, valid_time = prepare_wind(OUTPUT_DIR)
    arcpy.AddMessage(f"Raster: {raster}")
    arcpy.AddMessage(f"Model valid time: {valid_time.isoformat()}")
    if FEDERATED_SERVER:
        service_url = publish_wind(
            raster, valid_time, server_url=FEDERATED_SERVER, overwrite=OVERWRITE,
        )
        arcpy.AddMessage(f"Published: {service_url}")
        return service_url
    return raster


if __name__ == "__main__":
    run()
