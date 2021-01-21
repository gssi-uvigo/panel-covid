# Dashboard COVID-19 España
*Guillermo Barreiro Fernández*

*atlanTTic, Universidade de Vigo*

Este proyecto pretende recopilar toda la información disponible públicamente sobre la evolución de la pandemia del COVID-19 en España, con el fin de poder analizarla y mostrar los resultados al público, de manera objetiva, sencilla y visualmente atractiva.

La descarga y extracción de los datos está implementada con [Apache Airflow](https://airflow.apache.org), los cuales se almacenan para su posterior análisis y visualización en una base de datos [MongoDB](https://mongodb.com). Posteriormente, usando también Apache Airflow, se procede al análisis de los datos almacenados, cuyos resultados se almacenan en otra base de datos, también dentro del mismo servidor MongoDB. El despliegue de ambas herramientas se lleva a cabo con [Docker](https://docker.com), automatizando la orquestación de los contenedores con [Docker Compose](https://docs.docker.com/compose/).

## Despliegue

### Configuración inicial:

`docker-compose build`

`docker-compose -f docker-compose.yml -f docker/docker-compose.admin.yml run airflow-initializer`

`docker-compose up`

`docker exec -it covid-dashboard_airflow-scheduler_1 airflow users create --username admin --firstname`  *Tu nombre* `--lastname` *Tu apellido* `--role Admin --email` *Tu email*

### Lanzamiento de los contenedores:
`docker-compose up`

Una vez que todos los contenedores estén encendidos, Apache Airflow lanzará una vez por día el workflow de descarga, extracción y análisis de los datos.

## Estructura de archivos:
- `docker-compose.yml`: define los contenedores Docker que conforman este proyecto.
- `covid_data`: carpeta en la que se descargarán todos los datos. Mapeada con los contenedores de Airflow.
- `dags`: contiene los ficheros Python que componen el workflow de descarga, extracción y análisis. Mapeada con los contenedores de Airflow.
- `docker`: archivos de configuración varios del despliegue del proyecto con Docker Compose.

## Fuentes de datos
- **[Ministerio de Sanidad](https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/situacionActual.htm)**:
    - Informe PDF de evolución de la pandemia, publicado diariamente de lunes a viernes. *[Ejemplo del día 13 de enero de 2021](https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Actualizacion_289_COVID-19.pdf)*.
        - **Pruebas diagnósticas realizadas**: número de pruebas realizadas por día, tasa por cada 100000 y positividad por Comunidad Autónoma. *Número de pruebas realizadas disponible a partir del informe 189 (20/08/2020), y el resto de datos a partir del 204 (10/09/2020).*.
        - **Origen de los brotes**: número de brotes y casos acumulados por ámbito. *Disponible de forma esporádica en algunos informes: 230, 235, 240...*
        - **Presión hospitalaria**: datos diarios por Comunidad Autónoma del número de pacientes COVID hospitalizados, porcentaje de ocupación de camas UCI y totales por casos COVID, número de ingresos y número de altas. *Datos disponibles a partir del informe 189 (20/08/2020).*
- **[RENAVE](https://www.isciii.es/QueHacemos/Servicios/VigilanciaSaludPublicaRENAVE/EnfermedadesTransmisibles/Paginas/InformesCOVID-19.aspx)**:
    - [casos_hosp_uci_def_sexo_edad_provres.csv](https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv): CSV actualizado diariamente con el número de casos, hospitalizaciones, ingresos en UCI y fallecimientos diarios, agrupados por sexo, rango de edad y provincia.
    - Informe PDF de evolución epidemiológica, publicado semanalmente. *[Ejemplo del día 29 de diciembre de 2020](https://www.isciii.es/QueHacemos/Servicios/VigilanciaSaludPublicaRENAVE/EnfermedadesTransmisibles/Documents/INFORMES/Informes%20COVID-19/Informe%20COVID-19.%20Nº%2059_29%20de%20diciembre%20de%202020.pdf)*:
        - **Descripción clínica de los casos**: enfermedades previas y síntomas presentados. *Tabla 2, disponible desde el informe 12 (20/03/2020) hasta el 33 (29/05/2020).*
        - **Indicadores de transmisión**: porcentaje de casos asintomáticos, días hasta diagnóstico (mediana y rango intercuartil), contactos estrechos identificados por caso (mediana y rango intercuartil) y casos sin contacto estrecho conocido (número y porcentaje) por Comunidad Autónoma. *Tabla 6, disponible a partir del informe 34 (15/07/2020).*
- **[AEMET OpenData](https://opendata.aemet.es/centrodedescargas/inicio)**: API REST que ofrece datos diarios metereológicos de todo el país desde el 1 de marzo de 2020. El uso de la API requiere registro gratuito. *Información usada para buscar correlaciones entre la situación epidemiológica y meteorológica.*
- **[Google Community Mobility Reports](https://www.google.com/covid19/mobility/)**: Datos anonimizados de movilidad de los usuarios de Google a nivel mundial desde el inicio de la pandemia. *Información usada para buscar correlaciones entre la movilidad de los ciudadanos y la evolución de la pandemia.*
- **[INE (Instituto Nacional de Estadística)](http://ine.es)**:
    - Prevalencia de enfermedades crónicas entre los ciudadanos españoles: [descargar en CSV](https://www.ine.es/jaxi/files/_px/es/csv_bdsc/t00/mujeres_hombres/tablas_1/l0/d03005.csv_bdsc)
    - Causas de muerte en España en 2018: [descargar en CSV](http://www.ine.es/jaxi/files/_px/es/csv_sc/t15/p417/a2018/01004.csv_sc)
    - Población por Comunidad Autónoma, desglosada en sexo y rango de edad: [descargar en CSV](https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/9683.csv)
- **Creación propia**:
    - Provincias españolas agrupadas por Comunidad Autónoma: [descargar en CSV](https://gist.githubusercontent.com/gbarreiro/7e5c5eb906e9160182f81b8ec868bf64/raw/8812c03a94edc69f77a6c94312e40a05b0c19583/provincias_espa%25C3%25B1a.csv)

## Datos disponibles en la base de datos
- *covid_extracted_data*:
    - **Datos COVID**:
        - `daily_data`: Casos, hospitalizaciones y fallecimientos diarios y totales por día, sexo, Comunidad Autónoma y rango de edad.
        - `hospitals_pressure`: Presión hospitalaria por día y Comunidad Autónoma: número de pacientes ingresados, porcentaje de ocupación de camas, número de ingresos y número de altas.
        - `diagnostic_tests`: Número de pruebas diagnósticas realizadas por día, tasa por cada 100000 y positividad. Datos diarios por Comunidad Autónoma.
        - `transmission_indicators`: Porcentaje de casos asintomáticos, días hasta diagnóstico (mediana y rango intercuartil), contactos estrechos identificados por caso (mediana y rango intercuartil) y casos sin contacto estrecho conocido (número y porcentaje) por Comunidad Autónoma.
        - `outbreaks_description`: Número de brotes y casos acumulados por ámbito.
    - **Datos adicionales**:
        - `death_causes`: Número de personas fallecidas en España en 2018 por causa, sexo y rango de edad.
        - `chronic_illnesses`: Porcentaje de población mayor de 15 años que en 2017 sufría una enfermedad crónica. Datos clasificados por sexo y enfermedad.
        - `google_mobility`: Porcentaje de variación de la movilidad de los ciudadanos respecto a la media por día, Comunidad Autónoma y tipo de desplazamiento.
        - `population_ar`: Población española por Comunidad Autónoma, sexo y rango de edad.
        - `weather_ar`: Temperatura, precipitaciones y luz solar diarios por Comunidad Autónoma.+

- *covid_analyzed_data*: *mongodb://data_read:givemesomedata@localhost:12345/covid_admin*
    - `cases`: Casos nuevos y totales, por 100 000 habitantes, incidencia acumulada, media móvil, incremento diario, semanal y mensual... Datos clasificados por Comunidad Autónoma, sexo y rango de edad.
    - `deaths`: Fallecimientos nuevos y totales, por 100 000 habitantes, media móvil, incremento diario, semanal y mensual y tasa de mortalidad (últimas 2 semanas y total). Datos clasificados por Comunidad Autónoma, sexo y rango de edad.
    - `hospitalizations`: Hospitalizaciones nuevas y totales, por 100 000 habitantes, media móvil, incremento diario, semanal y mensual y tasa de hospitalización (últimas 2 semanas y total). Datos para UCI y total. Datos clasificados por Comunidad Autónoma, sexo y rango de edad.
    - `death_causes`: Número de personas fallecidas en España en 2018 por cada una de las 10 causas de muerte más letales en España, junto con el número de personas fallecidas de COVID-19 en 2020.


## Librerías utilizadas
- [PyPDF2](https://pypi.org/project/PyPDF2/): extracción de datos de los ficheros PDF.
- [BeautifulSoup4](https://pypi.org/project/beautifulsoup4/): obtención de las URLs de los informes PDFs de la RENAVE desde su página web.
- [Pandas](https://pypi.org/project/pandas/): análisis y transformación de los datos extraídos.
- [pymongo](https://pypi.org/project/pymongo/): lectura y escritura de los datos extraídos y analizados desde/en la base de datos.