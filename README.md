# Sampa LiDAR 3D

Repositório de processamento dos dados LiDAR 3D da Cidade de São Paulo (2017 e 2020)

## Introdução

Levantamentos LiDAR 3D são um cenário cada vez mais corriqueiro nas cidades ao redor do mundo. São Paulo, foi a primeira cidade brasileira que publicou os dados LiDAR 3D, e mais uma vez deixa a disposição para a pesquisa uma fonte interessante de dados sobre a morfologia urbana.

## Motivação

Os dados LiDAR 3D, também conhecidos como nuvens de pontos são são algo trivial de trabalhar, sobretudo em uma cidade com as dimensões de São Paulo. No entanto, a informação sobre a forma urbana pode ser extraída e processada afim de democratizar mais o acesso a esse levantamento.

## Objetivo

Portanto, o objetivo desse repositório é processar e disponibilizar resultados e análises de ambos os levantamentos, 2017 e 2020, afim de auxiliar e dialogar com as diversas disciplinas que necessitam de modelos gerados a partir dos levantamentos LiDAR 3D

### Objetivos específicos

* MDS (Modelo digital de Superfície) raster matricial de 50cm e 1m de 2017 e 2020
* MDT (Modelo digital de Terreno) raster matricial de 50cm e 1m de 2017 e 2020
* BHM (Building Height Model) ou modelo de altura das edificações em raster matricial de 2017 e 2020 (https://www.kaggle.com/datasets/centrodametropole/sao-paulo-building-height-model/)
* VHM (Vegetation Height Model) ou modelo de altura da vegetação em raster matricial de 2017 e 2020
* Comparação (delta) de BHM e VHM no período de 2017 até 2020
* Modelo de fiação e cabeamento aéreo da cidade

## MAterias e métodos 

Os MDS LiDAR 3D estão disponíveis para download pelo site do GeoSampa. Eles serão baixados e processados em NoteBooks utilizando Python e o projeto Pdal

## REsultados

Os resultados assim que prontos estarão disponobilizados pasta resultados
