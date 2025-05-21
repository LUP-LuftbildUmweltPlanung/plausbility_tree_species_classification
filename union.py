import geopandas as gpd
import os

def intersect_polygons(wz_wuchskl_ndomDiff, top3_classes_probs_spec, output_path=None):
    """
    Führt eine geometrische Intersection (Schnitt) zweier Shapefiles durch.
    Nur überlappende Flächen bleiben erhalten.
    """
    gdf1 = wz_wuchskl_ndomDiff
    gdf2 = top3_classes_probs_spec

    if gdf1.crs != gdf2.crs:
        gdf2 = gdf2.to_crs(gdf1.crs)

    print("Intersection wird durchgeführt")
    gdf_intersect = gpd.overlay(gdf1, gdf2, how="intersection")

    if output_path:
        shapefile_name = "union_wz_classification.shp"
        shapefile_path = os.path.join(output_path, shapefile_name)
        gdf_intersect.to_file(shapefile_path)

    return gdf_intersect

def filter_polygons(union_wz_classification, output_path=None, area_threshold=50):
    """
    Filtert Polygone nach Mindestfläche, gültigen IDs und optional nach Baumart-Kürzel (spec1).
    """
    print("Filtern der Polygone wird durchgeführt")
    gdf = union_wz_classification

    if "area_m2" not in gdf.columns:
        gdf["area_m2"] = gdf.geometry.area

    mask = (
            (gdf["area_m2"] >= area_threshold) &
            (gdf["spec1"].str.lower() != gdf["BAGR"].str.lower())
    )

    gdf_filtered = gdf[mask].copy()

    if output_path:
        shapefile_name = "post_union.shp"
        shapefile_path = os.path.join(output_path, shapefile_name)
        gdf_filtered.to_file(shapefile_path)

    return gdf_filtered
