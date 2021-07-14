"""
    Download some CSV and JSON datasets and store them in the database:
        - COVID-19 situation by RENAVE CSV
        - Death causes CSV
        - Population per Autonomous Region CSV
"""
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime as dt
import locale

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

        # Convert the date from String to Date type
        df['date'] = pd.to_datetime(df['date'])

        # Replace provinces with Autonomous Regions
        df = pd.merge(df, provinces_df, on='province')
        df = df.drop(columns=['province'])
        self.df = df.groupby(['gender', 'age_range', 'date', 'autonomous_region']).sum().reset_index()

        # Get the data for the whole country
        df_total = self.df.groupby(['gender', 'age_range', 'date']).sum().reset_index()
        df_total['autonomous_region'] = 'España'
        self.df = pd.concat([self.df, df_total])

        # Get the data for both genders
        df_total = self.df.groupby(['age_range', 'date', 'autonomous_region']).sum().reset_index()
        df_total['gender'] = 'total'
        self.df = pd.concat([self.df, df_total])

        # Get the data for all ages
        df_total = self.df.groupby(['gender', 'date', 'autonomous_region']).sum().reset_index()
        df_total['age_range'] = 'total'
        self.df = pd.concat([self.df, df_total])

        # Calculate the total cases, deaths, and hospitalizations
        df_total = self.df.groupby(['gender', 'age_range', 'date', 'autonomous_region']).sum().groupby(
            ['gender', 'age_range', 'autonomous_region']).cumsum().rename(
            columns={'new_cases': 'total_cases', 'new_hospitalizations': 'total_hospitalizations',
                     'new_ic_hospitalizations': 'total_ic_hospitalizations', 'new_deaths': 'total_deaths'})
        self.df = pd.merge(self.df, df_total, on=['gender', 'age_range', 'date', 'autonomous_region'])


class DiagnosticTestsDataset(CSVDataset):
    """Represent the Ministry of Health CSV with the daily data about diagnostic tests"""

    def __init__(self, diagnostic_tests_file, provinces_dataset_file):
        self.provinces_dataset_file = provinces_dataset_file
        df = pd.read_csv(diagnostic_tests_file, sep=';', encoding='iso-8859-1')
        super(DiagnosticTestsDataset, self).__init__(None, dataframe=df)

    def __process_dataset__(self):
        df = self.df
        provinces_df = pd.read_csv(self.provinces_dataset_file, sep=';')

        # Translate the indexes
        df = df.rename(
            columns={'PROVINCIA': 'province', 'FECHA_PRUEBA': 'date', 'N_ANT_POSITIVOS': 'antigens_positive',
                     'N_ANT': 'antigens_total', 'N_PCR_POSITIVOS': 'pcr_positive', 'N_PCR': 'pcr_total'})

        # Parse the date
        df['date'] = pd.to_datetime(df['date'], format='%d%b%Y')

        # Replace provinces with Autonomous Regions
        df = pd.merge(df, provinces_df, on='province')
        df = df.drop(columns=['province'])
        df = df.groupby(['date', 'autonomous_region']).sum().reset_index()

        # Calculate the total number of tests
        df['total_diagnostic_tests'] = df['antigens_total'] + df['pcr_total']

        # Calculate the positivity
        df['positivity'] = 100*((df['antigens_positive'] + df['pcr_positive']) / df['total_diagnostic_tests'])

        self.df = df


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


class DeathCausesDataset(CSVDataset):
    """Represent a dataset containing all the death causes in Spain in 2018"""

    age_range_translations = {'\*': '', ' *[\(\)] *': '', 'Todas las edades': 'total',
                              'Menos de ([0-9]*) año': '0-\\1', 'De ([0-9]*) a ([0-9]*) años': '\\1-\\2',
                              '([0-9]*) y más años': '\\1+'}
    column_name_translations = {'Causa de muerte': 'death_cause', 'Sexo': 'gender', 'Edad': 'age_range',
                                'Total': 'total_deaths'}
    gender_translations = {'Total': 'total', 'Hombres': 'M', 'Mujeres': 'F'}

    def __init__(self, file):
        df = pd.read_csv(file, sep=';', decimal=',', thousands='.')
        super().__init__(file, dataframe=df)

    def __process_dataset__(self):
        death_causes = self.df
        death_causes = death_causes[death_causes['Periodo'] == 2018].drop(
            columns='Periodo')  # we only need the death causes for 2018
        death_causes = death_causes.rename(columns=DeathCausesDataset.column_name_translations)  # translate the column
        # names to English
        death_causes['age_range'] = death_causes['age_range'].replace(DeathCausesDataset.age_range_translations,
                                                                      regex=True)  # convert the age ranges to a
        # numeric notation
        death_causes['age_range'] = death_causes['age_range'].apply(
            lambda x: x.strip())  # remove trailing spaces from the age ranges
        death_causes['death_cause'] = death_causes['death_cause'].replace({'[0-9A-Z\- ]+\.': ''},
                                                                          regex=True)  # normalize the death causes
        # (keep them in Spanish)
        death_causes['gender'] = death_causes['gender'].replace(DeathCausesDataset.gender_translations)  # translate the
        # gender to English

        self.df = death_causes


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

        download_diagnostic_tests_data_op = PythonOperator(task_id='download_daily_diagnostic_tests_data',
                                                           python_callable=CSVDatasetsTaskGroup.
                                                           download_diagnostic_tests_data,
                                                           task_group=self,
                                                           dag=dag)

        store_diagnostic_tests_data_op = PythonOperator(task_id='store_daily_diagnostic_tests_data',
                                                        python_callable=CSVDatasetsTaskGroup.
                                                        process_and_store_diagnostic_tests_data,
                                                        task_group=self,
                                                        dag=dag)

        download_death_causes_op = PythonOperator(task_id='download_death_causes',
                                                  python_callable=CSVDatasetsTaskGroup.download_death_causes,
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

        download_death_causes_op >> store_death_causes_op
        download_daily_data_op >> store_daily_data_op
        download_diagnostic_tests_data_op >> store_diagnostic_tests_data_op

    # region Downloads

    @staticmethod
    def download_daily_covid_data():
        """Download the RENAVE dataset with the daily cases, hospitalizations and deaths"""
        download_csv_file('https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv',
                          "daily_covid_data.csv")

    @staticmethod
    def download_population_and_provinces():
        """Download the datasets with the population on each Autonomous Region."""
        download_csv_file('https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/9683.csv', 'population_ar.csv', False)

    @staticmethod
    def download_death_causes():
        """Download the dataset with the death causes in Spain in 2018"""
        download_csv_file('https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/6609.csv', 'death_causes.csv', False)

    @staticmethod
    def download_diagnostic_tests_data():
        """Download the daily diagnostics tests data"""
        today = dt.today()
        filename = f'Datos_Pruebas_Realizadas_Historico_{today.strftime("%d%m%Y")}.csv'
        download_csv_file('https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/'
                          + filename, 'diagnostic_tests.csv')

    # endregion

    # region Data processing and storage

    @staticmethod
    def process_and_store_cases_and_deaths():
        dataset = DailyCOVIDData('csv_data/daily_covid_data.csv', '/home/airflow/provinces_daily_renave_data.csv')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'daily_data')

    @staticmethod
    def process_and_store_ar_population():
        dataset = ARPopulationCSVDataset("csv_data/population_ar.csv", separator=';', decimal=',', thousands='.')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'population_ar')

    @staticmethod
    def process_and_store_death_causes():
        dataset = DeathCausesDataset('csv_data/death_causes.csv')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'death_causes')

    @staticmethod
    def process_and_store_diagnostic_tests_data():
        dataset = DiagnosticTestsDataset('csv_data/diagnostic_tests.csv', '/home/airflow/'
                                                                          'provinces_daily_diagnostic_data.csv')
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        dataset.store_dataset(database, 'diagnostic_tests')

    # endregion
