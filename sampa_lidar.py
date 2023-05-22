import pandas as pd
import pdal
import json
import geopandas as gpd
import numpy as np
import glob
import sys
import os
import shutil

gdf_articulacao = gpd.read_file("zip://data/SIRGAS_SHP_quadriculamdt.zip!/SIRGAS_SHP_quadriculamdt/")
gdf_articulacao.set_crs(epsg=31983, inplace=True)

DATA_DIR_2020 = '/media/fernando/DATA/LiDAR-Sampa-2020'
DATA_DIR_2017 = '/media/fernando/DATA/LiDAR-Sampa-2017'

class Scm:

    def __init__(self, scm) -> None:
        
        coords = [[xy[0], xy[1]] for xy in gdf_articulacao.set_index('qmdt_cod').loc[scm].geometry.exterior.coords]
        xy_max = np.max(np.array(coords), axis=0) 
        xy_min = np.min(np.array(coords), axis=0)
        width, height = np.ceil(xy_max * 2) - np.ceil(xy_min * 2)

        origin_x, origin_y = np.floor(xy_min * 2)/2
        
        self.scm = scm
        self.width = width
        self.height = height
        self.origin_x = origin_x
        self.origin_y = origin_y

    

def pipeline(scm, ano):
    # Retorna o json com o Pipeline para o determinado SCM
    scm_att = Scm(scm)
    pipeline = [
        {
            "type": "readers.las",
            "filename": f'temp/{ano}-{scm}.laz',
            "override_srs": "EPSG:31983"
        },
        {
            "filename":f"results/MDS-{scm}-{ano}.tiff",
            "gdaldriver":"GTiff",
            "output_type":"max",
            "resolution":"0.5",
            "type": "writers.gdal",
            "gdalopts":"COMPRESS=ZSTD, PREDICTOR=3, BIGTIFF=YES",
            "width": scm_att.width,
            "height": scm_att.height,
            "origin_x": scm_att.origin_x,
            "origin_y": scm_att.origin_y,
            "default_srs": "EPSG:31983"
        },
        {
            "type": "filters.delaunay",
            "where": "(Classification == 2)"
        },
        {
            "type": "filters.faceraster",
            "resolution": 0.5,
            "width": scm_att.width,
            "height": scm_att.height,
            "origin_x": scm_att.origin_x,
            "origin_y": scm_att.origin_y,
        },
        {
            "filename":f"results/MDT-{scm}-{ano}.tiff",
            "type": "writers.raster",
            # "filename":f"results/DTM-{grid_number}.tiff",
            "gdaldriver":"GTiff",
            "data_type": "float32",
            "gdalopts":"COMPRESS=ZSTD, PREDICTOR=3, BIGTIFF=YES",
            "nodata":"0",
        },
        {
            "type":"filters.range",
            "limits":"Classification[4:6]"
        },
        {
            "type":"filters.hag_dem",
            "raster": f"results/MDT-{scm}-{ano}.tiff",
            "zero_ground": True
        },
        {
            "type":"filters.ferry",
            "dimensions":"HeightAboveGround => Z"
        },
        {
            "type":"filters.range",
            "limits":"Z[0:300]"
        },
        {
            "filename":f"results/BHM-{scm}-{ano}.tiff",
            "gdaldriver":"GTiff",
            "output_type":"max",
            "resolution":"0.5",
            "type": "writers.gdal",
            "gdalopts":"COMPRESS=ZSTD, PREDICTOR=3, BIGTIFF=YES",
            "width": scm_att.width,
            "height": scm_att.height,
            "origin_x": scm_att.origin_x,
            "origin_y": scm_att.origin_y,
            "data_type": "float32",
            "where": "(Classification == 6)",
            "default_srs": "EPSG:31983"
        },
        {
            "filename":f"results/VHM-{scm}-{ano}.tiff",
            "gdaldriver":"GTiff",
            "output_type":"max",
            "resolution":"0.5",
            "type": "writers.gdal",
            "gdalopts":"COMPRESS=ZSTD, PREDICTOR=3, BIGTIFF=YES",
            "width": scm_att.width,
            "height": scm_att.height,
            "origin_x": scm_att.origin_x,
            "origin_y": scm_att.origin_y,
            # "nodata":"0",
            "data_type": "float32",
            "where": "(Classification == 4 || Classification == 5)",
            "default_srs": "EPSG:31983"
        }
    ]
    return pipeline

def pipeline_2020(scm):
    # Retorna o json com o Pipeline para o determinado SCM
    return None


def processo(scm):
    ## TODO
    # Verifica se o processamento já foi relaizado para o SCM

    # Copia arquivos de 2017 e 2020 para uma pasta temporária
    file_2017 = glob.glob(f'{DATA_DIR_2017}/*{scm}*.laz')
    file_2020 = glob.glob(f'{DATA_DIR_2020}/*{scm}*.laz')
    if len(file_2017) != 1 or len(file_2020) != 1:
        raise ValueError(f'Os arquivos do {scm} parecem não conforme!')
    
    shutil.copy(file_2017[0], f'temp/2017-{scm}.laz')
    shutil.copy(file_2020[0], f'temp/2020-{scm}.laz')
  
    # Processa o PDAL para cada ano
        # MDT
        # MDS
        # BHM
        # VHM
    mdt_mds = pdal.Pipeline(json.dumps(pipeline(scm, 2017)))
    n_points = mdt_mds.execute()
    print(f'Executando MDT/MDS com {n_points} pontos')

    mdt_mds = pdal.Pipeline(json.dumps(pipeline(scm, 2020)))
    n_points = mdt_mds.execute()
    print(f'Executando MDT/MDS com {n_points} pontos')

    ## TODO
    # Copia os resultados para as pastas apropriadas

    # Exclui os arquivos da pasta temporária
    # os.remove(f'temp/2017-{scm}.laz')
    # os.remove(f'temp/2020-{scm}.laz')
    
    return None

def processa_tudo():
    # Itera sobre todos os SCMs
    # Utilizando multiprocessamento
    print(Scm('3315-361').origin_x)
    return None

def main():
    print('Processamento de Dados LIDAR Sampa 2017/2020 ***')
    if len(sys.argv) > 1:
        # Processa os SCMS na sequencia
        for scm in sys.argv[1:]:
            processo(scm)
    else:
        print(gdf_articulacao.shape)
        processa_tudo()

if __name__ == '__main__':
    main()