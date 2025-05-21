import geopandas as gpd
import os
import pandas as pd

def get_mode(series):
    try:
        return series.mode().iloc[0]
    except IndexError:
        return None

def compute_majority_spec(post_union, id_col, union_area_col, wzba_area_col, prob1_col, spec_col):
    """
    Liest das Shapefile ein, konvertiert relevante Spalten zu numerisch und berechnet die Mehrheit spec1 pro Fläche.
    """
    print("Prüfung der Plausibilität wird durchgeführt")
    gdf = post_union

    # Spalten sicher in numerisch umwandeln
    gdf[union_area_col] = pd.to_numeric(gdf[union_area_col], errors="coerce")
    gdf[wzba_area_col] = pd.to_numeric(gdf[wzba_area_col], errors="coerce")
    gdf[prob1_col] = pd.to_numeric(gdf[prob1_col], errors="coerce")

    gdf['majority_spec1'] = gdf.groupby(id_col)[spec_col].transform(get_mode)

    return gdf

def compute_mode_filtered_stats(gdf, id_col, union_area_col, prob1_col, spec_col):
    mode_filtered = gdf[gdf[spec_col] == gdf['majority_spec1']]

    sum_area = mode_filtered.groupby(id_col)[union_area_col].sum().reset_index(name='mode_sum_union_area')
    mean_prob1 = mode_filtered.groupby(id_col)[prob1_col].mean().reset_index(name='mean_prob1_for_majority_spec1')

    gdf = gdf.merge(sum_area, on=id_col, how='left')
    gdf = gdf.merge(mean_prob1, on=id_col, how='left')

    return gdf

def filter_gdf_by_area(gdf, wzba_area_col):
    union_area_sum_col = "mode_sum_union_area"
    return gdf[gdf[union_area_sum_col] >= gdf[wzba_area_col] / 2].copy()

def aggregate_final_values(gdf_filtered, id_col, wzba_area_col):
    return gdf_filtered.groupby(id_col).agg({
        'mean_prob1_for_majority_spec1': 'first',
        'majority_spec1': 'first',
        'BAGR': 'first',
        'BAGR1': 'first',
        'BAGR2': 'first',
        'mode_sum_union_area': 'first',
        wzba_area_col: 'first',
    }).reset_index()

def determine_plaus_spec(row):
    if row['mode_sum_union_area'] >= row['FLAECHE'] / 2:
        if row['mean_prob1_for_majority_spec1'] > 0.9:
            return row['majority_spec1']
        elif row['mean_prob1_for_majority_spec1'] > 0.7 and row['majority_spec1'] in (row['BAGR'], row['BAGR1'], row['BAGR2']):
            return row['majority_spec1']
    return None

def apply_plausibility(final_aggregated):
    final_aggregated['plaus_spec'] = final_aggregated.apply(determine_plaus_spec, axis=1)

    return final_aggregated

def merge_plaus_spec_to_wzba(wzba_path, final_aggregated, id_col, output_path=None):
    wz_ba_gdf = wzba_path
    wz_ba_gdf[id_col] = wz_ba_gdf[id_col].astype(final_aggregated[id_col].dtype)

    # Optionaler Filter vor dem Merge
    filtered = wz_ba_gdf[
        ((wz_ba_gdf['ndomDiff'] > 4) & (wz_ba_gdf['wuchskl'] != 3)) |
        ((wz_ba_gdf['BAGR'] == "uLW") & (wz_ba_gdf['wuchskl'] != 3)) |
        ((wz_ba_gdf['BAGR'] == "uNW") & (wz_ba_gdf['wuchskl'] != 3))
    ]

    # Merge nur für die ausgewählten IDs
    cols_to_merge = [id_col, 'plaus_spec', 'mean_prob1_for_majority_spec1']
    result_gdf = filtered.merge(final_aggregated[cols_to_merge], on=id_col, how='left')

    # === Korrektes Zurückschreiben in den Original-DataFrame ===
    wz_ba_gdf_updated = wz_ba_gdf.copy()

    for col in ['plaus_spec', 'mean_prob1_for_majority_spec1']:
        merged_values = result_gdf.set_index(id_col)[col]
        wz_ba_gdf_updated.loc[wz_ba_gdf_updated[id_col].isin(merged_values.index), col] = (
            wz_ba_gdf_updated[id_col].map(merged_values)
        )

    # === ERSETZUNGSLOGIK ===

    # 1. Gültige plaus_spec (nicht leer/na)
    valid_mask = wz_ba_gdf_updated["plaus_spec"].notna() & (
        wz_ba_gdf_updated["plaus_spec"].astype(str).str.strip() != ""
    )

    # 2. Unterschied zwischen BAGR und plaus_spec
    diff_mask = wz_ba_gdf_updated["BAGR"] != wz_ba_gdf_updated["plaus_spec"]

    # 3. Ersetze BAGR, wenn plaus_spec gültig und unterschiedlich
    wz_ba_gdf_updated.loc[valid_mask & diff_mask, "BAGR"] = wz_ba_gdf_updated.loc[valid_mask & diff_mask, "plaus_spec"]
    wz_ba_gdf_updated.loc[valid_mask & diff_mask, "plaus_spec"] = wz_ba_gdf_updated.loc[
        valid_mask & diff_mask, "plaus_spec"]

    # 4. Wenn plaus_spec == BAGR und sie nicht gerade erst ersetzt wurde → plaus_spec löschen
    same_mask = (wz_ba_gdf_updated["BAGR"] == wz_ba_gdf_updated["plaus_spec"]) & (~diff_mask)
    wz_ba_gdf_updated.loc[same_mask, "plaus_spec"] = None

    # === SPEICHERN ===
    if output_path:
        shapefile_name = "final_result.gpkg"
        shapefile_path = os.path.join(output_path, shapefile_name)
        wz_ba_gdf_updated.to_file(shapefile_path, driver="GPKG")

    return wz_ba_gdf_updated