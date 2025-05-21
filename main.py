# -*- coding: utf-8 -*-

from zonal_rasterstats import *
from classification_to_vector import *
from union import *
from plausibility import *
from postprocessing import *
from raster_output import *
import time
from functions import *
import shutil

### INPUT PARAMETERS ###
# Pfad zur WZ-Shapefile
WZ = r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input_new2\wz_ba_2011_2021_f.shp" # r"PATH/*.shp"

# Pfade für die notwendigen TIF-Dateien
TIF_DICT = {
    "wuchskl": r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input_new\wk_all_1_kor.img",
    "ndomDiff": r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input_new\bhk_abgleich_kat.img",
    "ndom": None # r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input\ndom2022_kor_test.tif"
}
CLASSIFICATION = r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input_new2\predict_thueringen_prob.tif"

# Output-Pfad, zum speichern der Ergebnisse
OUTPUT_PATH = r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\output_new2"

# Anpassung der beiden Shapefiles Überhälter Punkte und Überhälter Flächen
ANPASSUNG_UEBERHAELTER = False # Updated Überhälter Punkte & Fläche
UEBERHAELTER_P = r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input_new\wz_ueberhaelter_2011_2021_p.shp" # None falls nicht aktualisiert werden soll
UEBERHAELTER_F = r"C:\Users\frede\Documents\Projekte\Baumartenklassifikation\plausibilisierung\input_new\wz_ueberhaelter_2011_2021_f.shp" # None falls nicht aktualisiert werden soll

### Advanced Parameters ###
RESOLUTION = 1.0 # Auflösung zum Resampling der Input-Raster
METHOD = "nearest" # Resampling Methode (Optionen: nearest, bilinear, cubic)
CORES = 13 # Anzahl Prozessorkerne
MAX_DIST = 10 # Filter-Distanz der Zentroide des Klassifikationsrasters zu den Eingangspolygonen
CELL_SIZE = 10 # Auflösung des Classification-Rasters
OUTPUT_RASTER = False # Erstellt Raster mit einem Band, basierend auf dem höchsten Wert des Klassifikationsraster

# Maincode
if __name__ == "__main__":
    startzeit = time.time()

    #Create temp folder
    temp_folder = os.path.join(OUTPUT_PATH, "temp")
    os.makedirs(temp_folder, exist_ok=True)

    temp_folder_results = os.path.join(OUTPUT_PATH, "temp_results")
    os.makedirs(temp_folder_results, exist_ok=True)

    temp_folder_split = os.path.join(OUTPUT_PATH, "split")
    os.makedirs(temp_folder_results, exist_ok=True)

    results_folder = os.path.join(OUTPUT_PATH, "results")
    os.makedirs(results_folder, exist_ok=True)

    split_files = split_by_grid(WZ, temp_folder_split)
    resampled_dict = resample_rasters_from_dict(TIF_DICT, temp_folder, RESOLUTION, METHOD)

    for chunk_file in os.listdir(temp_folder_split):
        if chunk_file.endswith(".shp"):
            chunk_path = os.path.join(temp_folder_split, chunk_file)

            # Calculate zonal statistics (zonal_rasterstats.py)
            clipped_dict = clip_dict_to_patch(resampled_dict, chunk_path, temp_folder)
            wz_wuchskl_ndomDiff = calculate_zonal_stats(chunk_path, clipped_dict, CORES)

            # Transform classification raster into polygon grids and insert attribut values (classification_to_vector.py)
            classification = clip_raster_to_patch(CLASSIFICATION, chunk_path, temp_folder)
            classification_to_points = raster_to_points(classification, temp_folder)
            points_filtered = filter_points_by_distance(classification_to_points, chunk_path, MAX_DIST)
            polygon_grids = points_to_raster_cells(points_filtered, CELL_SIZE)
            class_mapping = {
                1: "FI",
                2: "KI",
                3: "LA",
                4: "BU",
                5: "EI",
                6: "BI",
                7: "ER",
                8: "ES",
                9: "SH",
                10: "SW",
                11: "SN"
            }
            top3_classes_probs_spec = extract_top_classes(polygon_grids, class_mapping)

            # Union and filtering (union.py)
            union_wz_classification = intersect_polygons(wz_wuchskl_ndomDiff, top3_classes_probs_spec)
            post_union = filter_polygons(union_wz_classification)

            # Plausibility (plausibility.py)
            # === Spaltennamen (einheitlich anpassen!) ===
            ID = "OBJECTID"
            union_area = "area_m2"
            wzba_area = "FLAECHE"
            bagr1 = "BAGR"
            bagr2 = "BAGR1"
            bagr3 = "BAGR2"
            spec1 = "spec1"
            prob1 = "prob1"

            # Mehrheitliche spec1 bestimmen
            gdf = compute_majority_spec(post_union, ID, union_area, wzba_area, prob1, spec1)
            # Flächensummen & mittlere Wahrscheinlichkeit der Mehrheitsklasse berechnen
            gdf = compute_mode_filtered_stats(gdf, ID, union_area, prob1, spec1)
            # Flächen filtern (mind. 50 % der Fläche mit gleicher spec1)
            gdf_filtered = filter_gdf_by_area(gdf, wzba_area)
            # Endgültige Aggregation
            final_aggregated = aggregate_final_values(gdf_filtered, ID, wzba_area)
            # Plausibilitätsregel anwenden
            final_aggregated = apply_plausibility(final_aggregated)
            # Mit ursprünglichen wz_ba-Flächen mergen (nur wo Bedingungen erfüllt sind)
            final_result = merge_plaus_spec_to_wzba(wz_wuchskl_ndomDiff, final_aggregated, ID)

            # Postprocessing (postprocessing.py)
            post_processed_1 = change_attributes(final_result)
            post_processed_2 = add_bhoeh(post_processed_1)
            post_processed_3 = change_attribute_for_bl(post_processed_2)
            # Attribute aktualisieren und löschen
            final_result_postprocessed = update_attributes(post_processed_3, temp_folder_results)

    # Merge results
    final = merge_shapefiles(temp_folder_results, results_folder)

    # Anpassung von Ueberhaelter Shapefiles
    if ANPASSUNG_UEBERHAELTER is True:
        update_ueberhaelter_p(UEBERHAELTER_P, final, results_folder)
        update_ueberhaelter_f(UEBERHAELTER_F, final, results_folder)

    # Anpassung und AUsgabe der Klassifikationsraster
    if OUTPUT_RASTER is True:
        compress_to_max_band_raster(CLASSIFICATION, results_folder)

    # Temp-Folder löschen
    print("Temporäre Ergebnisse werden gelöscht")
    shutil.rmtree(temp_folder)
    shutil.rmtree(temp_folder_results)
    shutil.rmtree(temp_folder_split)

    endzeit = time.time()
    print("process finished in " + str((endzeit - startzeit) / 60) + " minutes")