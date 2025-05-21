import rasterio
import geopandas as gpd
from shapely.geometry import Point, box
from shapely.strtree import STRtree
import numpy as np
import os
import pandas as pd
from shapely.strtree import STRtree
import shapely
import pyogrio

def raster_to_points(tif_path, output_path, layer_name="classification_to_points", batch_size=10000000):
    print("Starte speicherschonende Umwandlung des Rasters in Punkte...")

    if output_path is None:
        output_path = os.path.dirname(tif_path)
    gpkg_path = os.path.join(output_path, f"{layer_name}.gpkg")

    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)

    with rasterio.open(tif_path) as src:
        bands = src.read()  # shape: (11, rows, cols)
        transform = src.transform
        rows, cols = bands.shape[1:]
        crs = src.crs

        all_points = []
        all_data = []

        total_pixels = rows * cols
        processed = 0
        for row in range(rows):
            for col in range(cols):
                vals = bands[:, row, col]

                if np.any(np.isnan(vals)):
                    continue

                x, y = rasterio.transform.xy(transform, row, col)
                all_points.append(Point(x, y))
                all_data.append(vals.tolist())

                # Sicherheit: zu viele Punkte -> zwischendurch speichern
                if len(all_points) >= batch_size:
                    save_batch(all_points, all_data, gpkg_path, crs, layer_name, first_batch=(not os.path.exists(gpkg_path)))
                    all_points = []
                    all_data = []

                processed += 1
                if processed % 1_000_000 == 0:
                    percent = processed / total_pixels * 100
                    print(f"{processed:,} Punkte verarbeitet ({percent:.1f}%)")

        # Rest speichern
        if all_points:
            save_batch(all_points, all_data, gpkg_path, crs, layer_name, first_batch=(not os.path.exists(gpkg_path)))

    return gpkg_path

def save_batch(points, data, gpkg_path, crs, layer_name, first_batch):
    gdf = gpd.GeoDataFrame(
        data,
        columns=[f"cl{i+1}" for i in range(len(data[0]))],
        geometry=points,
        crs=crs
    )

    if first_batch:
        gdf.to_file(gpkg_path, layer=layer_name, driver="GPKG")
    else:
        gdf.to_file(gpkg_path, layer=layer_name, driver="GPKG", mode='a')

def filter_points_by_distance(classification_to_points, polygons_path, max_dist, output_path=None):
    print("Punkte werden gefiltert")
    #points = classification_to_points
    points = gpd.read_file(classification_to_points)
    polygons = gpd.read_file(polygons_path)

    if points.crs != polygons.crs:
        points = points.to_crs(polygons.crs)

    # STRtree erstellen für schnellere Abfragen
    tree = STRtree(polygons.geometry)

    # Funktion: Punkt innerhalb max_dist eines Polygons?
    def is_close_enough(point):
        # Nur Nachbarn suchen
        nearest = tree.query(point.buffer(max_dist))

        # Holen Sie sich die Geometrien der benachbarten Polygone
        nearest_geometries = [polygons.geometry.iloc[i] for i in nearest]

        # Prüfen Sie die Abstände zu den Geometrien
        return any(geom.distance(point) <= max_dist for geom in nearest_geometries)

    # Anwendung auf alle Punkte
    print("Prüfe Punkte")
    mask = [is_close_enough(geom) for geom in points.geometry]
    gdf = points[mask]

    # Ergebnis speichern
    if output_path:
        shapefile_name = "points_filtered.gpkg"
        shapefile_path = os.path.join(output_path, shapefile_name)
        gdf.to_file(shapefile_path, driver="GPKG")

    return gdf

def points_to_raster_cells(points_filtered, cell_size=10, output_path=None):
    print("konvertiere Punkte zu Polygonen")
    #gdf = gpd.read_file(points_path)
    gdf = points_filtered
    # Für jeden Punkt ein Rechteck erzeugen (centered 10x10m box)
    def point_to_cell(pt):
        x, y = pt.x, pt.y
        half = cell_size / 2
        return box(x - half, y - half, x + half, y + half)

    gdf['geometry'] = gdf.geometry.apply(point_to_cell)

    if output_path:
        shapefile_name = "polygon_grids.gpkg"
        shapefile_path = os.path.join(output_path, shapefile_name)
        gdf.to_file(shapefile_path, driver="GPKG")

    return gdf


def extract_top_classes(polygon_grids, class_mapping, output_path=None, top_n=3):
    """
    Extrahiert die top-n Klassen aus Klassifikationsspalten und fügt Klassennamen + Wahrscheinlichkeiten hinzu.

    Parameters:
        polygon_grids: Geodataframe aus voriger Berechnung points_to_raster_cells
        class_mapping (dict): Mapping von class index (1-based) zu Kürzeln, z. B. {1: 'FI', 2: 'BU', ...}.
        output_path (str): Wenn angegeben, wird die bearbeitete Shapefile gespeichert.
        top_n (int): Anzahl der Top-Klassen, die extrahiert werden sollen.

    Returns:
        gpd.GeoDataFrame: Ergebnis mit Top-Klassen und ihren Kürzeln + Wahrscheinlichkeiten.
    """
    print("Berechnung der Top-3 Klassen wird durchgeführt")
    gdf = polygon_grids
    num_classes = len(class_mapping)
    prob_columns = [f"cl{i}" for i in range(1, num_classes + 1)]

    def top_classes(row):
        idx = np.argsort(-row.values)[:top_n]
        probs = row.values[idx]
        classes = idx + 1
        specs = [class_mapping.get(i, f"cl{i}") for i in classes]
        return pd.Series(
            np.concatenate([classes, probs, specs]),
            index=(
                    [f"class{i + 1}" for i in range(top_n)] +
                    [f"prob{i + 1}" for i in range(top_n)] +
                    [f"spec{i + 1}" for i in range(top_n)]
            )
        )

    gdf = gdf.join(gdf[prob_columns].apply(top_classes, axis=1))
    gdf.drop(columns=prob_columns, inplace=True)

    if output_path:
        shapefile_name = "top3_classes_probs_spec.gpkg"
        shapefile_path = os.path.join(output_path, shapefile_name)
        gdf.to_file(shapefile_path, driver="GPKG")

    return gdf