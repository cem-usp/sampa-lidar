for f in presenca_de_pontos/*.tif; do
  echo "Atribuindo EPSG:31983 para $f"
  gdal_edit.py -a_srs EPSG:31983 "$f"
done