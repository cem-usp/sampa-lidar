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

def pipeline(scm):
    # Retorna o json com o Pipeline para o determinado SCM
    return None

def processo(scm):
    # Copia arquivos de 2017 e 2020 para uma pasta temporária
    file_2017 = glob.glob(f'{DATA_DIR_2017}/*{scm}*.laz')
    file_2020 = glob.glob(f'{DATA_DIR_2020}/*{scm}*.laz')
    if len(file_2017) != 1 or len(file_2020) != 1:
        raise ValueError(f'Os arquivos do {scm} parecem não conforme!')
    
    shutil.copy(file_2017[0], f'temp/2017-{scm}.laz')
    shutil.copy(file_2020[0], f'temp/2020-{scm}.laz')

    ## TODO
    # Verifica se o processamento já foi relaizado para o SCM
        
        # Processa o PDAL para cada ano
            # MDT
            # MDS
            # BHM
            # VHM
    # Copia os resultados para as pastas apropriadas

    # Exclui os arquivos da pasta temporária
    os.remove(f'temp/2017-{scm}.laz')
    os.remove(f'temp/2020-{scm}.laz')
    
    # return None

def processa_tudo():
    # Itera sobre todos os SCMs
    # Utilizando multiprocessamento
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