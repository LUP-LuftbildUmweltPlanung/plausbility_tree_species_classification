import geopandas as gpd
from shapely.geometry import box
import rasterio
from rasterio.mask import mask
import os
import pandas as pd

def split_by_grid(shapefile_path, output_dir, rows=3, cols=5):
    os.makedirs(output_dir, exist_ok=True)

    # Lade Original-Shapefile
    gdf = gpd.read_file(shapefile_path)
    gdf = gdf.reset_index(drop=True)

    # Gesamtausdehnung
    minx, miny, maxx, maxy = gdf.total_bounds

    # Höhe und Breite jeder Zelle
    cell_width = (maxx - minx) / cols
    cell_height = (maxy - miny) / rows

    patch_counter = 1
    for row in range(rows):
        for col in range(cols):
            # Bounding Box der aktuellen Zelle
            cell_minx = minx + col * cell_width
            cell_maxx = cell_minx + cell_width
            cell_miny = miny + row * cell_height
            cell_maxy = cell_miny + cell_height
            cell_box = box(cell_minx, cell_miny, cell_maxx, cell_maxy)

            # Schneide Gitterzelle mit den Polygonen
            patch = gdf[gdf.centroid.within(cell_box)]

            if not patch.empty:
                # Speichern
                patch_path = os.path.join(output_dir, f"patch_{patch_counter}.shp")
                patch.to_file(patch_path)

                print(f"Patch {patch_counter}: {len(patch)} Polygone")
                patch_counter += 1

    print("Aufteilung abgeschlossen.")

def clip_raster_to_patch(raster_path, shapefile_path, output_folder):
    print("Clip Raster zum Patch")
    # Dateinamen erzeugen
    raster_name = os.path.splitext(os.path.basename(raster_path))[0]
    patch_name = os.path.splitext(os.path.basename(shapefile_path))[0]
    output_path = os.path.join(output_folder, f"{patch_name}_{raster_name}_clipped.tif")

    # Patch laden
    gdf = gpd.read_file(shapefile_path)
    geoms = gdf.geometry.values

    # Raster clippen
    with rasterio.open(raster_path) as src:
        out_image, out_transform = mask(src, geoms, crop=True)
        out_meta = src.meta.copy()

    # Metadaten aktualisieren
    out_meta.update({
        "driver": "GTiff",
        "height": out_image.shape[1],
        "width": out_image.shape[2],
        "transform": out_transform
    })

    # Ordner anlegen und speichern
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with rasterio.open(output_path, "w", **out_meta) as dest:
        dest.write(out_image)

    return output_path

def clip_dict_to_patch(tif_dict, shapefile_path, output_folder):
    result_dict = {}
    for layer_name, raster_path in tif_dict.items():
        clipped_path = clip_raster_to_patch(raster_path, shapefile_path, output_folder)
        result_dict[layer_name] = clipped_path
    return result_dict

def merge_shapefiles(folder_path, output_path):
    shapefiles = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".gpkg")]

    merged_gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(f) for f in shapefiles], ignore_index=True), crs=gpd.read_file(shapefiles[0]).crs)

    # Datentyp ändern
    merged_gdf["ALTER_HOEH"] = merged_gdf["ALTER_HOEH"].astype(int)
    merged_gdf["ALTER_WDB"] = merged_gdf["ALTER_WDB"].astype(int)
    merged_gdf["SICHER"] = merged_gdf["SICHER"].astype(int)
    merged_gdf["ERRORCODE"] = merged_gdf["ERRORCODE"].astype(int)

    output_filename = "final_result_merged.shp"
    output_file = os.path.join(output_path, output_filename)
    merged_gdf.to_file(output_file)

    print(f"Merge abgeschlossen. Datei gespeichert unter: {output_file}")
    return merged_gdf