"""Build and publish the NOAA GFS 10 m wind layer from ArcGIS Pro Python."""

from datetime import datetime, timedelta, timezone
import json
from math import isclose
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from time import sleep
from urllib.error import HTTPError
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
GRID_SIZE = [1440, 721]


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
            raise ValueError("NOMADS returned a response without a GRIB signature.")
        output.write(signature)
        shutil.copyfileobj(response, output)


def wind_bands(info, cycle):
    if info.get("size") != GRID_SIZE:
        raise ValueError(f"Expected the global 1440 x 721 grid, got {info.get('size')}.")

    transform = info.get("geoTransform", [])
    if len(transform) != 6:
        raise ValueError("The input has no six-value geographic grid transform.")
    x0, dx, rx, y0, ry, dy = transform
    expected = (-180.0, 0.25, 0.0, 90.0, 0.0, -0.25)
    actual = (x0 + dx / 2, dx, rx, y0 + dy / 2, ry, dy)
    if not all(isclose(a, b, abs_tol=1e-5) for a, b in zip(actual, expected)):
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


def read_raster_info(gdal, path):
    info = gdal.Info(str(path), format="json")
    return json.loads(info) if isinstance(info, str) else info


def verify_tiff(gdal, path, cycle):
    info = read_raster_info(gdal, path)
    types = [band["type"] for band in info["bands"]]
    if wind_bands(info, cycle) != (1, 2) or types != ["Float32", "Float32"]:
        raise ValueError("The GeoTIFF must contain two Float32 bands ordered U, V.")


def convert_grib(gdal, grib, output, cycle):
    with gdal.ExceptionMgr(), gdal.config_option("GRIB_ADJUST_LONGITUDE_RANGE", "YES"):
        u_band, v_band = wind_bands(read_raster_info(gdal, grib), cycle)
        raster = gdal.Translate(
            str(output), str(grib), format="GTiff",
            bandList=[u_band, v_band], outputType=gdal.GDT_Float32,
            outputSRS="EPSG:4326", creationOptions=["COMPRESS=DEFLATE"],
        )
        raster = None  # Close and flush GDAL's output before ArcPy opens it.
        verify_tiff(gdal, output, cycle)


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
        if output.exists() and not grib_file:
            with gdal.ExceptionMgr():
                verify_tiff(gdal, output, model_time)
            return output, model_time

        with TemporaryDirectory(prefix="gfs-stage-", dir=output_dir) as temp_dir:
            stage = Path(temp_dir)
            grib = Path(grib_file) if grib_file else stage / "wind.grib2"
            if not grib_file:
                arcpy.AddMessage(f"Checking NOAA GFS {model_time.isoformat()}")
                try:
                    download_grib(model_time, grib)
                except HTTPError as error:
                    if error.code != 404 or index == len(cycles) - 1:
                        raise
                    sleep(10)
                    continue

            staged_tif = stage / name
            convert_grib(gdal, grib, staged_tif, model_time)
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
