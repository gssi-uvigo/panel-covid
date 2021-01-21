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
import pandas as pd
import numpy as np
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime as dt, timedelta as td

from AuxiliaryFunctions import download_csv_file, CSVDataset, MongoDatabase


# region CSV datasets models


class DailyCOVIDData(CSVDataset):
    """Represent the RENAVE CSV with the daily cases, hospitalizations and deaths"""

    def __init__(self, covid_dataset_file, provinces_dataset_file):
        self.provinces_dataset_file = provinces_dataset_file
        super(DailyCOVIDData, self).__init__(covid_dataset_file)

    def __process_dataset__(self):
        df = self.df
        provinces_df = pd.read_csv(self.provinces_dataset_file)

        # Translate the indexes
        df = df.rename(
            columns={'sexo': 'gender', 'provincia_iso': 'province', 'grupo_edad': 'age_range', 'fecha': 'date',
                     'num_casos': 'new_cases', 'num_def': 'new_deaths', 'num_hosp': 'new_hospitalizations',
                     'num_uci': 'new_ic_hospitalizations'})
        provinces_df = provinces_df.rename(columns={'iso': 'province', 'comunidad autónoma': 'autonomous_region'})

        # Translate the gender codes
        gender_translations = {'H': 'M', 'M': 'F', 'NC': 'unknown'}
        df['gender'] = df['gender'].replace(gender_translations)

        # Replace provinces with Autonomous Regions
        df = pd.merge(df, provinces_df, on='province')
        df = df.drop(columns=['province'])
        self.df = df.groupby(['gender', 'age_range', 'date', 'autonomous_region']).sum().reset_index()


class ARPopulationCSVDataset(CSVDataset):
    """Represents a CSV with the Spanish population classified by age and autonomous region"""

    def __process_dataset__(self):
        df_population_ar = self.df
        df_population_ar = df_population_ar[
            df_population_ar['Periodo'] == '1 de enero de 2020']  # get the population only for 2020
        df_population_ar = df_population_ar[
            df_population_ar['Nacionalidad'] == 'Total']  # get the population for every nationality
        df_population_ar.drop(columns=['Periodo', 'Nacionalidad'],
                              inplace=True)  # drop the columns used for the selection
        df_population_ar.rename(
            columns={'Comunidades y ciudades autonomas': 'autonomous_region', 'Grupo quinquenal de edad': 'age_range',
                     'Sexo': 'gender', 'Total': 'population'}, inplace=True)

        # Value replacements
        df_population_ar['autonomous_region'].replace(self.ar_translations,
                                                      inplace=True)  # replace the autonomous region name with the
        # appropriate translation
        df_population_ar['gender'].replace(self.gender_translations, inplace=True)  # translate the gender
        df_population_ar['age_range'] = df_population_ar['age_range'].apply(func=lambda x: x.strip()).replace(
            self.age_range_translations, regex=True)  # translate the age range

        # Pivot the table
        df_population_ar = df_population_ar.pivot(index=['autonomous_region', 'age_range'], columns='gender',
                                                  values='population')

        self.df = df_population_ar

        # Transform the DataFrame into a list of MongoDB documents
        population_ar_mongo = df_population_ar.to_dict('index')
        population_ar_mongo = [
            {'autonomous_region': x[0], 'age_range': x[1], 'M': y['M'], 'F': y['F'], 'total': y['total']}
            for x, y in population_ar_mongo.items()]

        self.mongo_data = population_ar_mongo


class AEMETWeatherDataset:
    """Represents a JSON dataset containing the weather for each day of the year in every weather station in Spain"""

    def __init__(self, weather_dataset, provinces_dataset):
        """
        Create a table with the average weather by Autonomous Region for each day of the year.
        :param weather_dataset: JSON file provided by AEMET
        :param provinces_dataset: CSV with the names of the Spanish provinces and the Autonomous Region they belong to.
        """

        # Load the weather and the provinces datasets
        weather_provinces = pd.read_json(weather_dataset)  # read the JSON with the weather data
        provinces_ar = pd.read_csv(provinces_dataset)  # read the CSV with the autonomous region for each province

        # Replace the province by the autonomous region
        weather_ar = pd.merge(weather_provinces, provinces_ar, on='provincia', how='left')

        # Rename the columns
        weather_ar = weather_ar[['fecha', 'comunidad autónoma', 'tmed', 'prec', 'sol']]
        weather_ar.rename(columns={'fecha': 'date', 'comunidad autónoma': 'autonomous_region', 'tmed': 'temperature',
                                   'prec': 'precipitations', 'sol': 'sunlight'}, inplace=True)

        # Column data types
        weather_ar.replace([np.nan, 'Ip', 'Acum'], "0.0", inplace=True)
        weather_ar[['precipitations', 'temperature', 'sunlight']] = weather_ar[
            ['precipitations', 'temperature', 'sunlight']].astype("float64")
        weather_ar['date'] = pd.to_datetime(weather_ar['date'])  # parse the date column

        # Calculate the average temperature for each autonomous region
        self.weather_ar = weather_ar.groupby(['autonomous_region', 'date'], as_index=False).mean()

    def store_dataset(self, database, collection_name):
        """Store the transformed data in the database"""
        database.store_data(collection_name, self.weather_ar.to_dict('records'))


class DeathCausesDataset(CSVDataset):
    """Represent a dataset containing all the death causes in Spain in 2018"""

    def __init__(self, file):
        df = pd.read_csv(file, sep=';', decimal=',', thousands='.', skiprows=5, skipfooter=6,
                         engine='python')
        super().__init__(file, dataframe=df)

    def __process_dataset__(self):
        death_causes_df = self.df
        death_causes_df.rename(columns={'Unnamed: 0': 'death_cause'}, inplace=True)  # rename the death cause column
        death_causes_df.drop(columns=['Unnamed: 23'], inplace=True)  # remove useless column

        # Replace the age ranges by a numeric format (XX-YY) and convert the multiple age range columns to a single one
        a = death_causes_df.melt(id_vars=['death_cause'], var_name='age_range', value_name='deaths')
        a = a.replace(CSVDataset.age_range_translations, regex=True)

        # Split the "death_cause" column into the actual death cause and three columns for male,
        # female and total deaths for that cause
        death_causes_df_pivoted = pd.DataFrame(columns=['death_cause', 'age_range', 'M', 'F', 'total'])
        genders = ['Ambos sexos', 'Hombres', 'Mujeres']
        current_cause = {}
        for index, row in a.iterrows():
            # Iterate row by row
            # There are two possibilities: death cause or total/female/male count for the current cause
            death_cause_gender = row['death_cause'].strip()
            if death_cause_gender not in genders:
                if current_cause:
                    death_causes_df_pivoted = death_causes_df_pivoted.append(current_cause, ignore_index=True)
                current_cause = {'death_cause': death_cause_gender, 'age_range': row['age_range']}
            elif death_cause_gender == 'Ambos sexos':
                current_cause['total'] = row['deaths']
            elif death_cause_gender == 'Hombres':
                current_cause['M'] = row['deaths']
            elif death_cause_gender == 'Mujeres':
                current_cause['F'] = row['deaths']

        death_causes_df = death_causes_df_pivoted

        # Remove numbers and other useless characters from the death cause
        death_causes_df['death_cause'] = death_causes_df['death_cause'].replace(['[0-9\-]+', '^ *[A-Z]+\.'], '',
                                                                                regex=True)
        death_causes_df['death_cause'] = death_causes_df['death_cause'].apply(lambda x: x.strip())

        # Replace NaN with 0
        death_causes_df.replace(np.nan, 0, inplace=True)

        self.df = death_causes_df


class ChronicIllnessesDataset(CSVDataset):
    """Represent a dataset containing the prevalence of chronic illnesses among Spanish population"""

    def __process_dataset__(self):
        chronic_df = self.df
        chronic_df.rename(
            columns={'Principales enfermedades crónicas o de larga evolución': 'illness', 'Sexo': 'gender',
                     'Total': 'percentage'}, inplace=True)
        chronic_df['gender'] = chronic_df['gender'].replace(CSVDataset.gender_translations)
        chronic_df = chronic_df.pivot(index=['illness'], columns='gender', values='percentage')
        chronic_mongo = [{'illness': x, 'M': y['M'], 'F': y['F']} for x, y in
                         chronic_df.to_dict('index').items()]

        self.df = chronic_df
        self.mongo_data = chronic_mongo


# endregion


class CSVDatasetsTaskGroup(TaskGroup):
    """TaskGroup that downloads some CSV and JSON datasets and store them in the database"""

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(CSVDatasetsTaskGroup, self) \
            .__init__("csv_datasets",
                      tooltip="Download and store in the database all the CSV datasets and the AEMET data",
                      dag=dag)

        # Instantiate the operators
        download_daily_data_op = PythonOperator(task_id='download_daily_covid_data',
                                                python_callable=CSVDatasetsTaskGroup.download_daily_covid_data,
                                                task_group=self,
                                                dag=dag)

        download_population_provinces_op = PythonOperator(task_id='download_population_provinces',
                                                          python_callable=CSVDatasetsTaskGroup.
                                                          download_population_and_provinces,
                                                          task_group=self,
                                                          dag=dag)

        download_death_causes_op = PythonOperator(task_id='download_death_causes',
                                                  python_callable=CSVDatasetsTaskGroup.download_death_causes,
                                                  task_group=self,
                                                  dag=dag)

        download_chronic_illnesses_op = PythonOperator(task_id='download_chronic_illnesses',
                                                       python_callable=CSVDatasetsTaskGroup.
                                                       download_chronic_illnesses_dataset,
                                                       task_group=self,
                                                       dag=dag)

        download_aemet_data_op = PythonOperator(task_id='download_aemet_data',
                                                python_callable=CSVDatasetsTaskGroup.download_aemet_data,
                                                task_group=self,
                                                dag=dag)

        store_aemet_data_op = PythonOperator(task_id='store_aemet_data',
                                             python_callable=CSVDatasetsTaskGroup.process_and_store_aemet_data,
                                             task_group=self,
                                             dag=dag)

        store_daily_data_op = PythonOperator(task_id='store_daily_data',
                                             python_callable=CSVDatasetsTaskGroup.process_and_store_cases_and_deaths,
                                             task_group=self,
                                             dag=dag)

        store_death_causes_op = PythonOperator(task_id='store_death_causes',
                                               python_callable=CSVDatasetsTaskGroup.process_and_store_death_causes,
                                               task_group=self,
                                               dag=dag)

        store_chronic_illnesses_op = PythonOperator(task_id='store_chronic_illnesses',
                                                    python_callable=CSVDatasetsTaskGroup.
                                                    process_and_store_chronic_illnesses,
                                                    task_group=self,
                                                    dag=dag)

        store_ar_population_op = PythonOperator(task_id='store_population_ar',
                                                python_callable=CSVDatasetsTaskGroup.process_and_store_ar_population,
                                                task_group=self,
                                                dag=dag)

        [download_aemet_data_op, download_population_provinces_op] >> store_aemet_data_op
        download_death_causes_op >> store_death_causes_op
        download_daily_data_op >> store_daily_data_op
        download_chronic_illnesses_op >> store_chronic_illnesses_op
        download_population_provinces_op >> store_ar_population_op

    # region Downloads

    @staticmethod
    def download_daily_covid_data():
        """Download the RENAVE dataset with the daily cases, hospitalizations and deaths"""
        download_csv_file('https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv',
                          "daily_covid_data.csv")

    @staticmethod
    def download_population_and_provinces():
        """Download the datasets with the population on each Autonomous Region and the Spanish provinces."""
        download_csv_file('https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/9683.csv', 'population_ar.csv', False)
        download_csv_file(
            'https://gist.githubusercontent.com/gbarreiro/7e5c5eb906e9160182f81b8ec868bf64/raw/'
            '8812c03a94edc69f77a6c94312e40a05b0c19583/provincias_espa%25C3%25B1a.csv', 'provinces_ar.csv', False)

    @staticmethod
    def download_death_causes():
        """Download the dataset with the death causes in Spain in 2018"""
        download_csv_file('http://www.ine.es/jaxi/files/_px/es/csv_sc/t15/p417/a2018/01004.csv_sc',
                          'death_causes.csv',
                          False)

    @staticmethod
    def download_chronic_illnesses_dataset():
        """Download the dataset with the prevalence of chronic illnesses among Spanish population in 2017"""
        download_csv_file(
            'https://www.ine.es/jaxi/files/_px/es/csv_bdsc/t00/mujeres_hombres/tablas_1/l0/d03005.csv_bdsc?nocab=1',
            'chronic_illnesses.csv', False)

    @staticmethod
    def download_aemet_data():
        """
            To get the AEMET weather data, multiple calls to the AEMET REST API have to be made, in 30-days time
            windows, hence the download and store process requires some more steps than with the other datasets.
        """
        # Base URL, endpoint URL and API key definition
        aemet_base_url = 'https://opendata.aemet.es/opendata'
        aemet_weather_endpoint = '/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{' \
                                 'fechaFinStr}/todasestaciones'
        aemet_api_key = 'eyJhbGciOiJIUzI1NiJ9' \
                        '.eyJzdWIiOiJndWlsbGVybW8uYmFycmVpcm9AZGV0LnV2aWdvLmVzIiwianRpIjoiOTYwNjA3NGQtNjBmYy00MWE4LThl'\
                        'MzQtMGNiY2MzODkzYmRiIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE2MDY4MzY1NjcsInVzZXJJZCI6Ijk2MDYwNzRkLTYwZ'\
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
            if first_request.json()['estado'] < 404:
                # The API response body contains a link to the actual content
                second_request_url = first_request.json()['datos']
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

    @staticmethod
    def process_and_store_cases_and_deaths():
        dataset = DailyCOVIDData('csv_data/daily_covid_data.csv', 'csv_data/provinces_ar.csv')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'daily_data')

    @staticmethod
    def process_and_store_ar_population():
        dataset = ARPopulationCSVDataset("csv_data/population_ar.csv", separator=';', decimal=',', thousands='.')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'population_ar')

    @staticmethod
    def process_and_store_aemet_data():
        dataset = AEMETWeatherDataset('csv_data/weather.json', 'csv_data/provinces_ar.csv')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'weather')

    @staticmethod
    def process_and_store_death_causes():
        dataset = DeathCausesDataset('csv_data/death_causes.csv')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'death_causes')

    @staticmethod
    def process_and_store_chronic_illnesses():
        dataset = ChronicIllnessesDataset('csv_data/chronic_illnesses.csv', separator=';', decimal=',')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'chronic_illnesses')

    # endregion
