"""
    Download the Google Mobility datasets and store them in the database.
"""

import os
import zipfile
import numpy as np
import pandas as pd

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from AuxiliaryFunctions import download_csv_file, CSVDataset, MongoDatabase


class GoogleMobilityDataset(CSVDataset):
    """Represent a dataset containing all the Spain mobility data provided by Google"""
    ar_translations = {'Andalusia': 'Andalucía', 'Aragon': 'Aragón', 'Balearic Islands': 'Baleares',
                       'Basque Country': 'País Vasco', 'Canary Islands': 'Canarias',
                       'Castile and León': 'Castilla y León', 'Castile-La Mancha': 'Castilla-La Mancha',
                       'Catalonia': 'Cataluña', 'Community of Madrid': 'Madrid', 'Navarre': 'Navarra',
                       'Region of Murcia': 'Murcia', 'Valencian Community': 'Comunidad Valenciana'}
    mobility_columns_rename = {'sub_region_1': 'autonomous_region',
                               'retail_and_recreation_percent_change_from_baseline': 'retail_and_recreation',
                               'grocery_and_pharmacy_percent_change_from_baseline': 'grocery_and_pharmacy',
                               'parks_percent_change_from_baseline': 'parks',
                               'transit_stations_percent_change_from_baseline': 'transit_stations',
                               'workplaces_percent_change_from_baseline': 'workplaces',
                               'residential_percent_change_from_baseline': 'residential'}

    def __process_dataset__(self):
        google_mobility_df = self.df
        google_mobility_df.drop(
            columns=['country_region_code', 'country_region', 'sub_region_2', 'metro_area', 'iso_3166_2_code',
                     'census_fips_code'], inplace=True)  # remove useless columns
        google_mobility_df.rename(columns=GoogleMobilityDataset.mobility_columns_rename,
                                  inplace=True)  # rename some columns
        google_mobility_df['autonomous_region'].replace(
            np.nan, 'España', inplace=True)  # tag the data for the whole country as "España"
        google_mobility_df['autonomous_region'].replace(
            GoogleMobilityDataset.ar_translations, inplace=True)  # translate the autonomous regions names to Spanish
        google_mobility_df['date'] = pd.to_datetime(google_mobility_df['date'])

        self.df = google_mobility_df


class GoogleDatasetsTaskGroup(TaskGroup):
    """TaskGroup that downloads and stores the Google Mobility datasets for Spain"""

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(GoogleDatasetsTaskGroup, self) \
            .__init__("google_mobility_datasets",
                      tooltip="Download and store in the database all the Google Mobility datasets",
                      dag=dag)

        # Instantiate the operators
        download_op = PythonOperator(task_id='download_google_mobility',
                                     python_callable=GoogleDatasetsTaskGroup.download_google_mobility,
                                     task_group=self,
                                     dag=dag)
        store_op = PythonOperator(task_id='process_google_mobility',
                                  python_callable=GoogleDatasetsTaskGroup.process_google_mobility,
                                  task_group=self,
                                  dag=dag)

        download_op >> store_op

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
    def process_google_mobility():
        """Process and store the Google mobility dataset in the database"""
        mobility_google = GoogleMobilityDataset('csv_data/google_mobility.csv')
        database = MongoDatabase()
        mobility_google.store_dataset(database, 'google_mobility')
