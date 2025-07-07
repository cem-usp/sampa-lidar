import pandas as pd
import pdal
import json
import geopandas as gpd
import numpy as np
import glob
import sys
import os
import shutil
from multiprocessing import Pool

gdf_articulacao = gpd.read_file("zip://data/SIRGAS_SHP_quadriculamdt.zip!/SIRGAS_SHP_quadriculamdt/")
# REMOVE SCM 3445-232, 3443-464 que não estão disponíveis para download em 2020

gdf_articulacao = gdf_articulacao[~gdf_articulacao.qmdt_cod.isin(['3445-232', '3443-464','2344-342', "3343-353", '2326-323', '3345-121', '2346-121'])]
gdf_articulacao.set_crs(epsg=31983, inplace=True)

DATA_DIR_2020 = '/media/fernando/DATA/LiDAR-Sampa-2020'
DATA_DIR_2017 = '/media/fernando/DATA/LiDAR-Sampa-2017'
RESULT_FOLDER = '/media/fernando/DATA/sampa-lidar'

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
            "filename":f"results/{ano}/BHMZ/BHMZ-{scm}-{ano}-1m.tiff",
            "gdaldriver":"GTiff",
            "output_type":"max",
            "resolution":"1",
            "type": "writers.gdal",
            "gdalopts":"COMPRESS=ZSTD, PREDICTOR=3, BIGTIFF=YES",
            "width": scm_att.width,
            "height": scm_att.height,
            "origin_x": scm_att.origin_x,
            "origin_y": scm_att.origin_y,
            "nodata":"0",
            "data_type": "float32",
            "where": "(Classification == 6)",
            "default_srs": "EPSG:31983"
        }
    ]
    return pipeline

def processo(scm):
    # Verifica se o processamento já foi realizado para o SCM
    # if len(glob.glob(f"{RESULT_FOLDER}/2017/MDS/MDS-{scm}-2017.tiff")) > 0 and len(glob.glob(f"{RESULT_FOLDER}/2020/MDS/MDS-{scm}-2020.tiff")) > 0:
    #     print(f'SCM {scm} processado anteriormente')
    #     return None

    if len(glob.glob(f"results/2017/BHMZ/BHMZ-{scm}-2017-1m.tiff")) > 0 and len(glob.glob(f"results/2020/BHMZ/BHMZ-{scm}-2020-1m.tiff")) > 0:
        print(f'SCM {scm} processado anteriormente')
        return None

    # Copia arquivos de 2017 e 2020 para uma pasta temporária
    file_2017 = glob.glob(f'{DATA_DIR_2017}/*{scm}*.laz')
    file_2020 = glob.glob(f'{DATA_DIR_2020}/*{scm}*.laz')
    if len(file_2017) != 1 or len(file_2020) != 1:
        raise ValueError(f'Os arquivos do {scm} parecem não conforme!')
    
    print(f'Processando {scm}')

    shutil.copy(file_2017[0], f'temp/2017-{scm}.laz')
    shutil.copy(file_2020[0], f'temp/2020-{scm}.laz')
  
    # Processa o PDAL para cada ano: MDT, MDS, BHM, VHM
    mdt_mds = pdal.Pipeline(json.dumps(pipeline(scm, 2017)))
    n_points = mdt_mds.execute()
    # print(f'Executando MDT/MDS com {n_points} pontos')

    mdt_mds = pdal.Pipeline(json.dumps(pipeline(scm, 2020)))
    n_points = mdt_mds.execute()
    # print(f'Executando MDT/MDS com {n_points} pontos')

    # Exclui os arquivos da pasta temporária
    os.remove(f'temp/2017-{scm}.laz')
    os.remove(f'temp/2020-{scm}.laz')
    
    print(f'Processado {scm}')
    
    return None

def processa_tudo():
    # Itera sobre todos os SCMs
    # Utilizando multiprocessamento
    scms = gdf_articulacao.loc[:, 'qmdt_cod'].to_list()
    with Pool(12) as p:
        _ = p.starmap(processo, zip(scms))
    # for scm in scms:
    #     processo(scm)
    return None

def main():
    print('Processamento de Dados LIDAR Sampa 2017/2020 ***')
    if len(sys.argv) > 1:
        # Processa os SCMS na sequencia
        for scm in sys.argv[1:]:
            if len(scm) == 4:
                scms = gdf_articulacao.loc[gdf_articulacao.qmdt_cod.str.startswith(scm), 'qmdt_cod'].to_list()
                with Pool(12) as p:
                    _ = p.starmap(processo, zip(scms))
            else:
                processo(scm)
    else:
        print(gdf_articulacao.shape)
        processa_tudo()

if __name__ == '__main__':
    main()