"""
    Program that downloads all the data needed for populating the COVID-19 database:
    - PDF Reports released by the Spanish Health Ministry -> msanidad_reports/
    - PDF Reports released by Red Nacional de Vigilancia Epidemiológica -> renave_reports/
    - CSV with the number of daily COVID cases, hospitalizations, IC admissions and deaths in Spain,
    grouped by province, age range and gender -> csv_data/covid_daily_data.csv
    - CSV with the population on each Autonomous Region -> csv_data/population_ar.csv
    - CSV with the provinces and Autonomous Regions in Spain -> csv_data/provinces.csv
    - CSV with the main causes of death in Spain in 2018 -> csv_data/death_causes.csv
    - CSV with the prevalence of chronic illnesses among Spanish population -> csv_data/chronic_illnesses.csv
    - JSON with the weather in Spain from March 2020, provided by AEMET -> csv_data/weather.json

    Require the BeautifulSoup library: pip install beautifulsoup4
"""
# region Libraries import
# Python internal libraries
import json
import os
import re
import zipfile
from datetime import datetime as dt, timedelta as td
import requests

# Python external libraries
from bs4 import BeautifulSoup

# Airflow
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# endregion

# region Auxiliary functions definition
def download_csv_file(url, filename, overwrite_if_exists=True):
    """
        Download a file from an URL and store it in the csv_data folder.
        :param url URL of the CSV file.
        :param filename name used to save the downloaded file.
        :param overwrite_if_exists When True, if the file already exists, it will be downloaded anyway and overwrite
        the previous one. When False, the download will be skipped.
    """

    # Create the directory for saving the CSVs
    if 'csv_data' not in os.listdir():
        os.mkdir('csv_data')

    if overwrite_if_exists or not os.path.exists('csv_data/' + filename):
        request = requests.get(url)
        if request.status_code < 400:
            print("File %s downloaded successfully" % filename)
            with open('csv_data/' + filename, 'wb') as file:
                file.write(request.content)
        else:
            print("Error downloading file %s" % filename)


# endregion


class DataDownloadTaskGroup(TaskGroup):
    """DAG that downloads all the data needed for populating the COVID-19 database"""

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(DataDownloadTaskGroup, self).__init__("data_download", tooltip="Download all the needed datasets",
                                                    dag=dag)

        # Instantiate the operators
        PythonOperator(task_id='download_mhealth_reports',
                       python_callable=DataDownloadTaskGroup.download_mhealth_reports,
                       task_group=self)
        PythonOperator(task_id='download_renave_reports',
                       python_callable=DataDownloadTaskGroup.download_renave_reports,
                       task_group=self)
        PythonOperator(task_id='download_csv_files',
                       python_callable=DataDownloadTaskGroup.download_csv_files,
                       task_group=self
                       )
        PythonOperator(task_id='download_google_mobility',
                       python_callable=DataDownloadTaskGroup.download_google_mobility,
                       task_group=self)
        PythonOperator(task_id='download_aemet_data',
                       python_callable=DataDownloadTaskGroup.download_aemet_data,
                       task_group=self)

    @staticmethod
    def download_mhealth_reports():
        """Download all the PDF reports released by the Ministry of Health"""

        url = 'https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Actualizacion_' \
              '{index}_COVID-19.pdf'

        report_number = 55  # the first report to work with will be the number 55 (25th March 2020)

        if 'mhealth_reports' not in os.listdir():
            os.mkdir('mhealth_reports')  # create the folder for downloading the reports

        while True:
            # Download report by report, until there are no more available
            if '{}.pdf'.format(report_number) not in os.listdir('mhealth_reports'):
                # Just download the report if it hasn't been downloaded yet
                print("Downloading report %i " % report_number)
                request = requests.get(url.format(index=report_number))
                if request.status_code < 400:
                    with open("mhealth_reports/{number}.pdf".format(number=report_number), "wb") as file:
                        file.write(request.content)
                else:
                    # No more reports available
                    break
            report_number += 1

    @staticmethod
    def download_renave_reports():
        """Download all the PDF reports released by the RENAVE"""

        base_url = 'https://www.isciii.es'

        # The latest reports are listed in one webpage, and the oldest in other
        new_reports_url = base_url + '/QueHacemos/Servicios/VigilanciaSaludPublicaRENAVE/EnfermedadesTransmisibles' \
                                     '/Paginas/InformesCOVID-19.aspx'
        old_reports_url = base_url + '/QueHacemos/Servicios/VigilanciaSaludPublicaRENAVE/EnfermedadesTransmisibles' \
                                     '/Paginas/-COVID-19.-Informes-previos.aspx'

        if 'renave_reports' not in os.listdir():
            os.mkdir('renave_reports')  # create the folder for downloading the reports

        link_title_placeholder = "informe nº"

        # Now let's download the HTML of the reports list for searching the PDFs URLs through HTML scrapping
        for url in [old_reports_url, new_reports_url]:
            # Download the HTML page
            html_reports = requests.get(url)

            # Scrap the HTML and look for the PDF links
            soup_reports = BeautifulSoup(html_reports.content, 'html.parser')
            links_reports = soup_reports.find_all('a', href=re.compile('QueHacemos/.*\.pdf'))
            links_html = [(x.get('href'), x.text.replace('\xa0', ' ')) for x in links_reports]
            links = {}

            for link_href, link_title in links_html:
                if link_title_placeholder in link_title.lower():
                    index_start = link_title.lower().index(link_title_placeholder)
                    index_end = link_title.index('.')
                    number = int(link_title[index_start + len(link_title_placeholder):index_end])
                    links[number] = link_href

            # Download the PDFs
            for number, report_url in links.items():
                print("Downloading report number %i: %s" % (number, report_url.replace('%20', ' ')))
                if not os.path.exists('renave_reports/{number}.pdf'.format(number=number)):
                    # Download the file only if it had not been previously downloaded
                    request = requests.get(base_url + report_url)
                    if request.status_code < 400:
                        with open("renave_reports/{number}.pdf".format(number=number), "wb") as file:
                            file.write(request.content)

    @staticmethod
    def download_csv_files():
        """Download all the CSV datasets"""
        # Daily updated datasets
        download_csv_file('https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv',
                          "covid_daily_data.csv")

        # Static datasets (only have to be downloaded once)
        download_csv_file('https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/9683.csv', 'population_ar.csv', False)
        download_csv_file(
            'https://gist.githubusercontent.com/gbarreiro/7e5c5eb906e9160182f81b8ec868bf64/raw/'
            '8812c03a94edc69f77a6c94312e40a05b0c19583/provincias_espa%25C3%25B1a.csv', 'provinces_ar.csv', False)
        download_csv_file('http://www.ine.es/jaxi/files/_px/es/csv_sc/t15/p417/a2018/01004.csv_sc', 'death_causes.csv',
                          False)
        download_csv_file(
            'https://www.ine.es/jaxi/files/_px/es/csv_bdsc/t00/mujeres_hombres/tablas_1/l0/d03005.csv_bdsc?nocab=1',
            'chronic_illnesses.csv', False)

    @staticmethod
    def download_google_mobility():
        """
            The Google Mobility dataset comes as ZIP file, hence the download and store process requires some more steps
            than with the other datasets.
        """
        google_mobility_filepath = 'csv_data/google_mobility.csv'
        google_mobility_zippath = 'csv_data/google_mobility.zip'

        # Remove the previous file
        if os.path.exists(google_mobility_filepath):
            os.remove(google_mobility_filepath)

        # Download the zip file
        download_csv_file('https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip',
                          'google_mobility.zip')

        # Unzip it
        with zipfile.ZipFile(google_mobility_zippath, 'r') as zip_file:
            zip_file.extract('2020_ES_Region_Mobility_Report.csv', 'csv_data/')  # extract only the dataset for Spain

        # Rename the dataset
        os.rename('csv_data/2020_ES_Region_Mobility_Report.csv', google_mobility_filepath)

        # Remove the zip file
        os.remove(google_mobility_zippath)

    @staticmethod
    def download_aemet_data():
        """
            To get the AEMET weather data, multiple calls to the AEMET REST API have to be made, in 30-days time
            windows, hence the download and store process requires some more steps than with the other datasets.
        """
        # Base URL, endpoint URL and API key definition
        aemet_base_url = 'https://opendata.aemet.es/opendata'
        aemet_weather_endpoint = '/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/' \
                                 '{fechaFinStr}/todasestaciones'
        aemet_api_key = 'eyJhbGciOiJIUzI1NiJ9' \
                        '.eyJzdWIiOiJndWlsbGVybW8uYmFycmVpcm9AZGV0LnV2aWdvLmVzIiwianRpIjoiOTYwNjA3NGQtNjBmYy00MWE4LThl' \
                        'MzQtMGNiY2MzODkzYmRiIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE2MDY4MzY1NjcsInVzZXJJZCI6Ijk2MDYwNzRkLTYwZ' \
                        'mMtNDFhOC04ZTM0LTBjYmNjMzg5M2JkYiIsInJvbGUiOiIifQ.JyL4G-tCZZIWRsJp5HBOMNdkPWE1rTS3vTkBu2CdI6c'

        # The data will be returned in JSON format
        headers = {'Content-Type': 'application/json', 'api_key': aemet_api_key}

        if 'csv_data' not in os.listdir():
            os.mkdir('csv_data')

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
            print(f'Downloading data from {from_string} to {until_string}')

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
