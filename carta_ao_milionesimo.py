import geopandas as gpd
from shapely.geometry import box
import pandas as pd
import numpy as np

# Parâmetros da CIM
lon_start, lon_end = -180, 180
lat_start, lat_end = -84, 84
step_lon = 6
step_lat = 4

# Letras de A a V
linhas = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:21]

# Geração da grade
geoms = []
nomes = []

for i, lat in enumerate(np.arange(lat_start, lat_end, step_lat)):
    for j, lon in enumerate(np.arange(lon_start, lon_end, step_lon)):
        cell = box(lon, lat, lon + step_lon, lat + step_lat)
        nome = f"{linhas[20 - i]}-{j + 1:02d}"  # Inverte latitude (de norte para sul)
        geoms.append(cell)
        nomes.append(nome)

# Criação do GeoDataFrame
gdf = gpd.GeoDataFrame({'nome_folha': nomes, 'geometry': geoms}, crs="EPSG:4326")

# Salvar o arquivo
gdf.to_file("carta_ao_milionesimo_1M.geojson", driver="GeoJSON")



escalas = {
    1000000: {
        "latitude": {
            "graus": 4,
            "linhas": 40,
            "nomenclatura": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:21]
        },
        "longitude": {
            "graus": 6,
            "linhas": 60,
            "nomenclatura": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:21]
        }
    },
    500000: {
        "latitude": {
            "graus": 2,
            "linhas": 2,
        },
        "longitude": {
            "graus": 3,
            "linhas": 2,
        },
        "nomenclatura": ["VX", "YZ"] 
    },
    250000: {
        "latitude": {
            "graus": 1,
            "linhas": 2,
            "nomenclatura": "AB"
        },
        "longitude": {
            "graus": 1.5,
            "linhas": 2,
            "nomenclatura": "CD"
        }
    },
    100000: {

    },
    50000: {

    },
    25000: {

    },
    10000: {

    }, 
    5000: {

    }, 
    2000: {

    }
}
