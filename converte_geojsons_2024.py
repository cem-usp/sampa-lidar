import geopandas as gpd
import pandas as pd
import glob
import os

# Caminho da pasta com os arquivos GeoJSON
pasta = "/Users/fernandogomes/dev/LiDAR-Sampa-2024"
arquivos_geojson = glob.glob(os.path.join(pasta, "*.geojson"))

gdfs = []

for caminho in arquivos_geojson:
    nome = os.path.splitext(os.path.basename(caminho))[0]  # remove extensão
    gdf = gpd.read_file(caminho)
    gdf["nome_arquivo"] = nome
    gdfs.append(gdf)

# Concatena todos
gdf_completo = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

# Define ou converte para EPSG:31983
gdf_completo = gdf_completo.set_crs("EPSG:31983", allow_override=True)

# Exporta para GPKG
saida = os.path.join(pasta, "poligonos_consolidados.gpkg")
gdf_completo.to_file(saida, driver="GPKG")

print(f"✔️ Arquivo salvo com sucesso: {saida}")
