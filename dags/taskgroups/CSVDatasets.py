"""
    Download some CSV and JSON datasets and store them in the database:
        - COVID-19 situation by RENAVE CSV
        - Death causes CSV
        - Chronic illnesses CSV
        - Population per Autonomous Region CSV
        - Provinces per Autonomous Regions CSV
        - Weather data by AEMET (REST API)
"""
import json
import os
import requests
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime as dt, timedelta as td

from AuxiliaryFunctions import download_csv_file


class CSVDatasetsTaskGroup(TaskGroup):
    """TaskGroup that downloads some CSV and JSON datasets and store them in the database"""

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(CSVDatasetsTaskGroup, self) \
            .__init__("csv_datasets",
                      tooltip="Download and store in the database all the CSV datasets and the AEMET data",
                      dag=dag)

        # Instantiate the operators
        download_csv_files_op = PythonOperator(task_id='download_csv_files',
                                               python_callable=CSVDatasetsTaskGroup.download_csv_files,
                                               task_group=self,
                                               dag=dag)
        download_aemet_data_op = PythonOperator(task_id='download_aemet_data',
                                                python_callable=CSVDatasetsTaskGroup.download_aemet_data,
                                                task_group=self,
                                                dag=dag)

    # region Downloads

    @staticmethod
    def download_csv_files():
        """Download all the CSV datasets"""
        # Daily updated datasets
        download_csv_file('https://cnecovid.isciii.es/covid19/resources/datos_ccaas.csv', "new_cases")
        download_csv_file('https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019'
                          '/ccaa_covid19_fallecidos_por_fecha_defuncion_nueva_serie_long.csv', "new_deaths")

        # Static datasets (only have to be downloaded once)
        download_csv_file('https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/9683.csv', 'population_ar.csv', False)
        download_csv_file(
            'https://gist.githubusercontent.com/gbarreiro/7e5c5eb906e9160182f81b8ec868bf64/raw'
            '/5a6864eedb92b2853db9f7e79156e73308e00c99/provincias_espa%25C3%25B1a.csv', 'provinces_ar.csv', False)
        download_csv_file('http://www.ine.es/jaxi/files/_px/es/csv_sc/t15/p417/a2018/01004.csv_sc',
                          'death_causes.csv',
                          False)
        download_csv_file(
            'https://www.ine.es/jaxi/files/_px/es/csv_bdsc/t00/mujeres_hombres/tablas_1/l0/d03005.csv_bdsc?nocab=1',
            'chronic_illnesses.csv', False)

    @staticmethod
    def download_aemet_data():
        """
            To get the AEMET weather data, multiple calls to the AEMET REST API have to be made, in 30-days time windows,
            hence the download and store process requires some more steps than with the other datasets.
        """
        # Base URL, endpoint URL and API key definition
        aemet_base_url = 'https://opendata.aemet.es/opendata'
        aemet_weather_endpoint = '/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{' \
                                 'fechaFinStr}/todasestaciones'
        aemet_api_key = 'eyJhbGciOiJIUzI1NiJ9' \
                        '.eyJzdWIiOiJndWlsbGVybW8uYmFycmVpcm9AZGV0LnV2aWdvLmVzIiwianRpIjoiOTYwNjA3NGQtNjBmYy00MWE4LThl' \
                        'MzQtMGNiY2MzODkzYmRiIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE2MDY4MzY1NjcsInVzZXJJZCI6Ijk2MDYwNzRkLTYwZ' \
                        'mMtNDFhOC04ZTM0LTBjYmNjMzg5M2JkYiIsInJvbGUiOiIifQ.JyL4G-tCZZIWRsJp5HBOMNdkPWE1rTS3vTkBu2CdI6c'

        # The data will be returned in JSON format
        headers = {'Content-Type': 'application/json', 'api_key': aemet_api_key}

        # The API endpoint only allows requests in a time window of 30 days, that's why the data will be obtained
        # iteratively
        file_path = 'csv_data/weather.json'
        data = []
        now = dt.now()
        aemet_date_format = '%Y-%m-%dT%H:%M:%SUTC'

        # If there is already a JSON with some data, just request the data from its last update date
        if os.path.exists(file_path):
            # Get the last date
            with open(file_path, 'r', encoding='latin-1') as f:
                json_dataset = json.load(f)
                from_date = dt.fromisoformat(json_dataset[-1]['fecha']) + td(days=1)
        else:
            # Start from the 1st March 2020 (beginning of the pandemic in Spain)
            from_date = dt(2020, 3, 1)

        while (now - from_date).days > 0:
            # Get the weather data for the next 30 days
            number_of_days = min((now - from_date).days, 30)
            until_date = from_date + td(days=number_of_days)
            from_string = from_date.strftime(aemet_date_format)
            until_string = until_date.strftime(aemet_date_format)
            print(f'{from_string} - {until_string}')

            first_request = requests.get(
                aemet_base_url + aemet_weather_endpoint.format(fechaIniStr=from_string, fechaFinStr=until_string),
                headers=headers)
            if first_request.status_code < 400:
                # The API response body contains a link to the actual content
                second_request_url = json.loads(first_request.content)['datos']
                second_request = requests.get(second_request_url, headers=headers)
                if second_request.status_code < 400:
                    # Append the response to the previous responses
                    data.extend(second_request.json())

            from_date = until_date + td(days=1)

        # Replace the comma decimal separator by a point (for later processing the data with Pandas)
        for item in data:
            for key in item:
                item[key] = item[key].replace(',', '.')

        # Store all the data in a JSON file
        if os.path.exists(file_path):
            with open(file_path, 'r') as original_file:
                original_json = json.load(original_file)
                original_json.extend(data)
                data = original_json

        with open(file_path, 'w') as new_file:
            new_file.write(json.dumps(data))

    # endregion

    # region Data processing and storage



    # endregion
