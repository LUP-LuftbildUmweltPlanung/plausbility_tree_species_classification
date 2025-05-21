# -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 09:52:04 2021

@author: Administrator
"""

import itertools
import multiprocessing
import geopandas as gpd
from rasterstats import zonal_stats
import fiona
import numpy as np
import os
import rasterio
from rasterio.enums import Resampling

def resample_raster(tif_path, output_folder, resolution, method):
    with rasterio.open(tif_path) as src:
        x_res, y_res = src.res

        # Wenn das Raster bereits 1x1m Auflösung hat → nichts tun
        if abs(x_res - resolution) < 1e-6 and abs(y_res - resolution) < 1e-6:
            print(f"TIFF hat bereits 1m Auflösung: {tif_path}")
            return tif_path

        # Zielpfad vorbereiten
        if output_folder is None:
            output_folder = os.path.dirname(tif_path)

        base_name = os.path.splitext(os.path.basename(tif_path))[0]
        out_path = os.path.join(output_folder, base_name + (f"_{resolution}m.tif"))

        print(f"Resampling von {tif_path} auf {resolution}m Auflösung")

        # Neue Breite und Höhe berechnen
        scale_x = x_res / resolution
        scale_y = y_res / resolution
        new_width = int(src.width * scale_x)
        new_height = int(src.height * scale_y)

        # Transform anpassen
        transform = src.transform * src.transform.scale(
            src.width / new_width,
            src.height / new_height
        )

        profile = src.profile.copy()
        profile.update({
            "height": new_height,
            "width": new_width,
            "transform": transform
        })

        # Resampling Methode definieren
        resampling_method = getattr(Resampling, method)

        # Neues Raster schreiben
        with rasterio.open(out_path, "w", **profile) as dst:
            for i in range(1, src.count + 1):
                dst.write(
                    src.read(
                        i,
                        out_shape=(new_height, new_width),
                        resampling=resampling_method
                    ),
                    i
                )
        print("Resampling abgeschlossen")
        return out_path

def resample_rasters_from_dict(tif_dict, output_folder, resolution, method):
    """Resample alle TIFFs im Dictionary auf 1m Auflösung, wenn nötig.

    Args:
        tif_dict (dict): Dictionary mit {name: tif_path}
        output_folder (str, optional): Zielordner für resamplete TIFFs.
                                       Standard: gleiche Ordner wie Eingabe.

    Returns:
        dict: Neues Dictionary mit {name: resampled_tif_path}
    """
    resampled_dict = {}
    for name, tif_path in tif_dict.items():
        if tif_path is not None:
            resampled_path = resample_raster(tif_path, output_folder, resolution, method)
            resampled_dict[name] = resampled_path
    return resampled_dict


def chunks(data, n):
    """Yield successive n-sized chunks from a slice-able iterable."""
    for i in range(0, len(data), n):
        yield data[i:i + n]

def zonal_stats_partial(args):
    feats, tif_path, name = args

    if name == "ndom":
        return zonal_stats(
            feats, tif_path,
            stats="max",
            add_stats={'mean_above_80': mean_above_80th_percentile},
            all_touched=True)
    else:
        return zonal_stats(
            feats,
            tif_path,
            stats="majority",
            all_touched=True
        )

# Define the custom function
def mean_above_80th_percentile(array):
    # Konvertiere in schreibbares Numpy-Array und filtere NaNs
    array = np.array(array).copy()
    array = array[~np.isnan(array)]

    if len(array) == 0:
        return np.nan

    # Berechne das 80. Perzentil
    percentile_80 = np.percentile(array, 80)
    values_above_80 = array[array > percentile_80]

    return np.mean(values_above_80) if len(values_above_80) > 0 else np.nan

def run_zonal_stats_parallel(features, tif_path, cores, name):
    p = multiprocessing.Pool(cores)
    args = [(chunk, tif_path, name) for chunk in chunks(features, cores)]
    stats_lists = p.map(zonal_stats_partial, args)
    p.close()
    p.join()
    return list(itertools.chain(*stats_lists))

def calculate_zonal_stats(shapefile_path, tif_dict, cores, output_path=None):
    multiprocessing.freeze_support()  # Wichtig für Windows

    with fiona.open(shapefile_path) as src:
        features = list(src)

    # Bestehende Datei laden oder Original
    shape = gpd.read_file(shapefile_path)

    # Für jedes Raster Attribut berechnen und speichern
    for name, tif_path in tif_dict.items():
        print(f"Berechne zonal_stats für: {name}")
        stats = run_zonal_stats_parallel(features, tif_path, cores, name)
        if name == "ndom":
            if tif_path is not None:
                shape[name] = [a["mean_above_80"] for a in stats]
        else:
            shape[name] = [a["majority"] for a in stats]

    shape["OBJECTID"] = range(1, len(shape) + 1)

    # Ausgabe vorbereiten
    if output_path:
        shapefile_name = "added_values.shp"
        shapefile_path = os.path.join(output_path, shapefile_name)
        shape.to_file(shapefile_path)

    return shape