import rasterio
import numpy as np
import os

def compress_to_max_band_raster(input_path, output_path):
    with rasterio.open(input_path) as src:
        data = src.read()
        max_vals = np.nanmax(data, axis=0)

        # Skalieren und NaNs → 255
        scaled = np.round(max_vals * 100)
        scaled[np.isnan(max_vals)] = 255
        scaled = scaled.astype(np.uint8)

        meta = src.meta.copy()
        meta.update({
            "count": 1,
            "dtype": "uint8",
            "nodata": 255  # gültiger NoData-Wert für uint8
        })

        # Dateiname für das Ergebnis erzeugen
        filename = os.path.splitext(os.path.basename(input_path))[0] + "_maxband.tif"
        output_path = os.path.join(output_path, filename)

        with rasterio.open(output_path, 'w', **meta) as dst:
            dst.write(scaled, 1)

    print(f"Neues 8-Bit-Raster gespeichert: {output_path}")
