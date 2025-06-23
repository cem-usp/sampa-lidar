"""
laz_to_presence_raster.py
-------------------------------------------------------------
Converte cada arquivo LAZ de uma pasta em um raster GeoTIFF de
presença/ausência (0/1), com 1 m de pixel, usando até 12 núcleos.

Saída: um .tif por .laz na pasta destino.
"""
from pathlib import Path
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import pdal   # bindings Python do PDAL

# =============== CONFIGURAÇÕES ================= #
ANO = 2017
PASTA_LAZ   = Path(f"../LiDAR-Sampa-{ANO}")       # pasta onde estão os .laz
PASTA_OUT   = Path(f"../LiDAR_produtos/{ANO}/presenca_de_pontos")   # pasta de saída dos .tif
PIXEL_SIZE  = 1.0                 # metros
N_CORES     = 12                  # núcleos a empregar
# ================================================= #

PASTA_OUT.mkdir(exist_ok=True)
os.environ["PDAL_NUM_THREADS"] = str(N_CORES)  # threads internas do PDAL


def build_pipeline(in_laz: Path, out_tif: Path) -> str:
    """Cria um pipeline PDAL (JSON) que gera raster 0/1."""
    pipeline = {
        "pipeline": [
            str(in_laz),
            {
                "type": "writers.gdal",
                "filename": str(out_tif),
                "resolution": PIXEL_SIZE,
                "output_type": "count",     # conta pontos por pixel
                "data_type": "uint16",      # suporta até 65535
                "nodata": 0
            }
        ]
    }
    return json.dumps(pipeline)


def process_laz(laz_path: Path) -> str:
    """Executa o pipeline para um único LAZ; retorna mensagem de status."""
    out_tif = PASTA_OUT / f"{laz_path.stem}.tif"
    if out_tif.exists():
        return f"⏩ já existia {out_tif.name}"

    pipe = pdal.Pipeline(build_pipeline(laz_path, out_tif))
    try:
        pipe.execute()
        return f"✅ gerado {out_tif.name}"
    except RuntimeError as e:
        return f"❌ erro em {laz_path.name}: {e}"


def main() -> None:
    laz_files = sorted(PASTA_LAZ.glob("*.laz"))
    total = len(laz_files)
    if total == 0:
        raise SystemExit(f"Nenhum .laz encontrado em {PASTA_LAZ.resolve()}")

    print(f"→ Arquivos LAZ encontrados: {total}")
    print(f"→ Processando com até {N_CORES} núcleo(s)\n")

    processed = 0
    with ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_laz, f): f for f in laz_files}
        for future in as_completed(futures):
            processed += 1
            print(future.result())
            print(f"Progresso: {processed}/{total}  |  faltam {total - processed}\n")

    print("🏁 Processamento concluído.")


if __name__ == "__main__":
    main()
