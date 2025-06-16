import os
import glob
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import json

origem = "/Users/fernandogomes/dev/Voo_IGC_2024/02_Laser_LiDAR"  # Caminho da pasta de origem
destino = "/Users/fernandogomes/dev/LiDAR-Sampa-2024"  # Caminho da pasta de destino
epsg_code = "EPSG:31983"  # Altere conforme o sistema de coordenadas dos seus dados

os.makedirs(destino, exist_ok=True)
# arquivos_las = glob.glob(os.path.join(origem, "*.las"))
# Filtra apenas os arquivos que ainda não foram processados (i.e., .laz não existe)
arquivos_las = []
for las_file in glob.glob(os.path.join(origem, "*.las")):
    nome_arquivo_laz = os.path.basename(las_file).replace(".las", ".laz")
    caminho_laz = os.path.join(destino, nome_arquivo_laz)
    if not os.path.exists(caminho_laz):
        arquivos_las.append(las_file)

def converter_para_geojson(arquivo_de_origem):
    # Caminhos de entrada e saída
    input_path = arquivo_de_origem
    # output_path = "/mnt/data/SF-23-Y-D-IV-1-NO-D-I-4.geojson"
    geojson_file = os.path.basename(input_path).replace(".json", ".geojson")
    output_path = os.path.join(destino, geojson_file)

    # Carrega o JSON
    with open(input_path, "r") as f:
        data = json.load(f)

    # Extrai o polígono
    polygon_geom = data["boundary"]["boundary_json"]

    # Monta o GeoJSON manualmente
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": polygon_geom["coordinates"]
                },
                "properties": {}
            }
        ]
    }

    # Salva como .geojson
    with open(output_path, "w") as f:
        json.dump(feature_collection, f)



def fix_las_global_encoding(filepath):
    with open(filepath, 'r+b') as f:
        f.seek(6)
        f.write(bytes([17, 0, 0, 0]));
    print(f"Arquivo corrigido: {filepath}")


def converter_para_laz(caminho_las):
    nome_arquivo = os.path.basename(caminho_las).replace(".las", ".laz")
    caminho_laz = os.path.join(destino, nome_arquivo)
    shape = os.path.basename(caminho_las).replace(".las", ".json")
    caminho_json = os.path.join(destino, shape)


    pipeline = {
        "pipeline": [
            {
                "type": "readers.las",
                "filename": caminho_las,
                "override_srs": epsg_code,
                "ignore_vlr": True
            },
            {
                "type": "writers.las",
                "filename": caminho_laz,
                "compression": "laszip"
            }
        ]
    }

    try:
        fix_las_global_encoding(caminho_las)
        subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            # input=str(pipeline).replace("'", '"').encode(),
            input=json.dumps(pipeline).encode(),
            check=True
        )
        # Salva a geometria da nuvem de pontos
        with open(caminho_json, "w") as outfile:
            subprocess.run(
                ["pdal", "info", "--boundary", caminho_laz],
                stdout=outfile,
                check=True
            )
        converter_para_geojson(caminho_json)
        os.remove(caminho_json)
        return f"✔️ Sucesso: {nome_arquivo}"
    except subprocess.CalledProcessError as e:
        return f"❌ Erro: {nome_arquivo} - {e}"

if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=12) as executor:
        futuros = [executor.submit(converter_para_laz, arq) for arq in arquivos_las]
        for futuro in as_completed(futuros):
            print(futuro.result())
    # break