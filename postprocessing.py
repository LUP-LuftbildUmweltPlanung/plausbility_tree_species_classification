import geopandas as gpd
import pandas as pd
import os
from datetime import datetime, date


def change_attributes(final_result, output_path=None):
    """
    Aktualisiert HERKUNFT, HOLZART, BA, DATUM und DATUM_DFE anhand plausibler Spezies.
    """
    print("Postproccessing wird durchgeführt")

    # Shapefile einlesen
    gdf = final_result

    # Nur Zeilen, wo plaus_spec gefüllt ist (≠ NaN und ≠ leere Zeichenkette)
    plaus_col = "plaus_spec"
    mask = gdf[plaus_col].notna() & (gdf[plaus_col].astype(str).str.strip() != "")

    # HERKUNFT setzen
    gdf.loc[mask, "HERKUNFT"] = "Baumartengruppe aus Sentinel-2 Klassifikation"

    # HOLZART setzen nach Baumart
    lbh_values = {"BI", "BU", "EI", "ER", "ES", "SW", "SH"}
    ndh_values = {"FI", "KI", "LA", "SN"}

    gdf.loc[mask & gdf[plaus_col].isin(lbh_values), "HOLZART"] = "LBH"
    gdf.loc[mask & gdf[plaus_col].isin(ndh_values), "HOLZART"] = "NDH"

    # BA entfernen
    gdf["BA"] = "oA"

    # Aktuelles Datum setzen
    today_datetime = pd.to_datetime(datetime.now().date())  # sichert richtigen Typ
    gdf.loc[mask, "DATUM"] = today_datetime

    # DATUM_DFE anpassen
    aktuelles_jahr = datetime.now().year
    datum_dfe = date(aktuelles_jahr, 1, 1)
    #gdf.loc[mask, "DATUM_DFE"] = pd.to_datetime(datum_dfe).date()
    gdf.loc[mask, "DATUM_DFE"] = pd.to_datetime(f"{aktuelles_jahr}-01-01")

    # speichern
    if output_path:
        shapefile_name = "final_result_postprocessed_test.gpkg"
        shapefile_path = os.path.join(output_path, shapefile_name)
        gdf.to_file(shapefile_path, driver="GPKG")

    return gdf

def add_bhoeh(gdf):
    """
    Fügt die Spalte BHOEH_DFE hinzu, berechnet als: round(ndom * 2) / 2
    """
    ndom_col = "ndom"
    new_col = "BHOEH_DFE"

    if ndom_col not in gdf.columns:
        return gdf

    gdf[new_col] = pd.to_numeric(gdf[ndom_col], errors="coerce").apply(lambda x: round(x * 2) / 2 if pd.notna(x) else None)
    return gdf

def change_attribute_for_bl(gdf):
    """
    Setzt HOLZART, BA und DATUM für wuchskl == 3.
    Alle anderen Attribute (außer 'geometry', 'HOLZART', 'BA') werden gelöscht.
    """
    bl_mask = gdf["wuchskl"] == 3

    # Setze 'BL' in HOLZART und BA
    gdf.loc[bl_mask, "HOLZART"] = "BL"
    gdf.loc[bl_mask, "BA"] = "BL"
    # Ändere das Attribut DATUM
    today = datetime.now().strftime("%d.%m.%Y")  # Format "15.05.2025"
    gdf.loc[bl_mask, "DATUM"] = today

    # Alle Spalten außer 'geometry', 'HOLZART', 'BA'
    columns_to_clear = [col for col in gdf.columns if col not in ("HOLZART", "BA", "Shape", "OBJECTID", "FLAECHE", "WZ_OA",
                                                                  "Shape_Leng", "Shape_Area", "area_m2", "BEARBEITER",
                                                                  "DATUM", "DATUM_DFE", "geometry", "wuchskl")]

    for col in columns_to_clear:
        gdf.loc[bl_mask, col] = None  # oder "" je nach Bedarf

    return gdf

def update_attributes(gdf, output_path):
    prob_col = "mean_prob1_for_majority_spec1"
    plaus_col = "plaus_spec"

    # Neue PROB-Spalte als ganzzahlig (0–100), falls plaus_spec gesetzt ist
    gdf["PROB"] = gdf.apply(
        lambda row: round(row[prob_col] * 100) if pd.notna(row[plaus_col]) and pd.notna(row[prob_col]) else pd.NA,
        axis=1
    ).astype("Int64")  # <- Int64 erlaubt NA + Integer

    # Spalte 'PROB' hinter 'HERKUNFT4' verschieben
    if "HERKUNFT4" in gdf.columns:
        cols = list(gdf.columns)
        cols.remove("PROB")
        insert_index = cols.index("HERKUNFT4") + 1
        cols.insert(insert_index, "PROB")
        gdf = gdf[cols]

    # nicht benötigte Spalten löschen
    columns_to_delete = ["mean_prob1_for_majority_spec1", "wuchskl", "ndomDiff", "OBJECTID", "plaus_spec"]
    gdf = gdf.drop(columns=columns_to_delete)

    # Speichern mit Zeitstempel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shapefile_name = f"final_result_{timestamp}.gpkg"
    shapefile_path = os.path.join(output_path, shapefile_name)
    gdf.to_file(shapefile_path, driver="GPKG")

    print(f"Prozess abgeschlossen. Ergebnis gespeichert unter: {shapefile_path}")
    return gdf

def update_ueberhaelter_p(ueberhaelter_p, polygons_gdf, output_path):
    """
    Aktualisiert vorhandene Attribute in points_gdf basierend auf räumlicher Zuordnung zu polygons_gdf.
    """
    print("Ueberhaelter_p wird angepasst")
    points_gdf = gpd.read_file(ueberhaelter_p)
    attributes_to_transfer = ["BAGR", "BA", "HOLZART"]

    # Spatial Join: Punkte bekommen Attribute von Polygonen, in denen sie liegen
    joined = gpd.sjoin(
        points_gdf,
        polygons_gdf[attributes_to_transfer + ["geometry"]],
        how="left",
        predicate="within"
    )

    # Bestehende Attribute aktualisieren
    for attr in attributes_to_transfer:
        joined_col = f"{attr}_right"
        points_gdf[attr] = joined[joined_col].values

    shapefile_name = "wz_ueberhaelter_p_aktualisiert.shp"
    shapefile_path = os.path.join(output_path, shapefile_name)
    points_gdf.to_file(shapefile_path)

    return points_gdf


def update_ueberhaelter_f(ueberhaelter_f, polygons_gdf, output_path):
    print("Ueberhaelter_f wird angepasst")
    ueberhaelter_gdf = gpd.read_file(ueberhaelter_f)
    attributes_to_transfer = ["BAGR", "BA", "HOLZART"]
    id_col = "FID"
    # Schritt 1: ID-Spalte sicherstellen
    if id_col not in ueberhaelter_gdf.columns:
        ueberhaelter_gdf = ueberhaelter_gdf.reset_index().rename(columns={"index": id_col})

    # Schritt 2: Geometrischer Schnitt
    intersection = gpd.overlay(ueberhaelter_gdf[[id_col, "geometry"]], polygons_gdf[attributes_to_transfer + ["geometry"]],
                               how="intersection")

    # Schritt 3: Fläche berechnen
    intersection["area"] = intersection.geometry.area

    # Schritt 4: Für jedes Zielpolygon den größten Überlappungseintrag finden
    idx_largest = intersection.groupby(id_col)["area"].idxmax()
    majority_matches = intersection.loc[idx_largest]

    # Schritt 5: Auswahl der Spalten
    mapping_df = majority_matches[[id_col] + attributes_to_transfer]

    # Schritt 6: Join mit originalem target_gdf
    updated_gdf = ueberhaelter_gdf.merge(mapping_df, on=id_col, how="left")

    # Schritt 7: Spalten anpassen
    for attr in attributes_to_transfer:
        col_x = f"{attr}_x"
        col_y = f"{attr}_y"
        if col_y in updated_gdf.columns:
            updated_gdf[attr] = updated_gdf[col_y]
            updated_gdf.drop([col_x, col_y], axis=1, inplace=True)

    # Nach dem Merge → Spalten sortieren wie im Original + neue ganz hinten
    original_order = list(ueberhaelter_gdf.columns)
    new_columns = [col for col in updated_gdf.columns if col not in original_order]
    updated_gdf = updated_gdf[original_order + new_columns]

    shapefile_name = "wz_ueberhaelter_f_aktualisiert.shp"
    shapefile_path = os.path.join(output_path, shapefile_name)
    updated_gdf.to_file(shapefile_path)

    return updated_gdf