import os
from abc import abstractmethod

import requests
from datetime import datetime as dt
from pymongo import ASCENDING, DESCENDING
import pandas as pd

from airflow.providers.mongo.hooks.mongo import MongoHook


class MongoDatabase:
    """
        Instance of the MongoDB database where all the extracted data will be stored.
        The database URI must be set in the "Connections" of Apache Airflow.
    """

    def __init__(self):
        """
            Connect to the database.
        """
        self.client = MongoHook(conn_id='mongo_covid').get_conn()
        self.db = self.client.get_database()

    def store_data(self, collection_name, data, additional_indexes=None, default_index="date"):
        """Store data in the database. The index for the collection will be compounded by the 'date' field and
        optionally the provided additional indexes"""

        if data:
            print("Storing the data in the database...")
            collection = self.db[collection_name]
            index = [(default_index, DESCENDING)]
            if additional_indexes:
                for add_index in additional_indexes:
                    index.append((add_index, ASCENDING))

            collection.create_index(index)
            collection.insert_many(data)

    def delete_data(self, collection_name):
        """Delete the previously stored data in a collection. This is useful for the data extracted from the CSV."""
        collection = self.db[collection_name]
        collection.delete_many({})

    def check_last_data_inserted(self, report_name):
        """Get the report index of the last piece of data inserted in a specific collection."""
        collection = self.db['data_extraction_checkpoints']
        last_update = collection.find_one({'report': report_name})
        if last_update:
            # There is already data stored in the requested collection
            return last_update['index']
        else:
            # There is no data for this collection yet
            return 0

    def insert_new_checkpoint(self, report_name, index):
        """Create a checkpoint with the last data extraction that has been stored into the database"""
        collection = self.db['data_extraction_checkpoints']
        query = {'report': report_name}
        last_update = collection.find_one(query)
        new_update = {"index": index, "updated": dt.now()}

        if last_update:
            # Update the previous checkpoint
            collection.update_many(query, {"$set": new_update})
        else:
            # Create a checkpoint
            collection.insert_one({**query, **new_update})

    def __del__(self):
        """When the object is destroyed, the connection with the MongoDB server is released"""
        self.client.close()


class CSVDataset:
    """Represent a dataset stored in a CSV file"""

    # Translation codes
    ar_translations = {'Total Nacional': 'España', '01 Andalucía': 'Andalucía', '02 Aragón': 'Aragón',
                       '03 Asturias, Principado de': 'Asturias', '04 Balears, Illes': 'Baleares',
                       '05 Canarias': 'Canarias',
                       '06 Cantabria': 'Cantabria', '07 Castilla y León': 'Castilla y León',
                       '08 Castilla - La Mancha': 'Castilla-La Mancha', '09 Cataluña': 'Cataluña',
                       '10 Comunitat Valenciana': 'Comunidad Valenciana', '11 Extremadura': 'Extremadura',
                       '12 Galicia': 'Galicia', '13 Madrid, Comunidad de': 'Madrid', '14 Murcia, Región de': 'Murcia',
                       '15 Navarra, Comunidad Foral de': 'Navarra', '16 País Vasco': 'País Vasco',
                       '17 Rioja, La': 'La Rioja', '18 Ceuta': 'Ceuta', '19 Melilla': 'Melilla'}
    ar_codes = {'AN': 'Andalucía', 'AR': 'Aragón', 'AS': 'Asturias', 'CB': 'Cantabria', 'CE': 'Ceuta',
                'CL': 'Castilla y León', 'CM': 'Castilla-La Mancha', 'CN': 'Canarias', 'CT': 'Cataluña',
                'EX': 'Extremadura', 'GA': 'Galicia', 'IB': 'Baleares', 'MC': 'Murcia', 'MD': 'Madrid', 'ML': 'Melilla',
                'NC': 'Navarra', 'PV': 'País Vasco', 'RI': 'La Rioja', 'VC': 'Comunidad Valenciana'}
    gender_translations = {'Hombres': 'M', 'Mujeres': 'F', 'Ambos sexos': 'total'}
    age_range_translations = {'\*': '', ' *[\(\)] *': '', 'Todas las edades': 'total',
                              'Menores de ([0-9]*) año': '0-\\1', 'De ([0-9]*) a ([0-9]*) años': '\\1-\\2',
                              'De ([0-9]*) años y más': '≥\\1', '([0-9]*) y más años': '≥\\1'}

    def __init__(self, file, separator=',', decimal='.', thousands=',', dataframe=None):
        self.df = dataframe if isinstance(dataframe, pd.DataFrame) else \
            pd.read_csv(file, sep=separator, decimal=decimal, thousands=thousands)
        self.mongo_data = None
        self.__process_dataset__()

    @abstractmethod
    def __process_dataset__(self):
        """Transform the DataFrame to extract the important information"""

    def store_dataset(self, database, collection_name, indexes=None):
        """Store the dataset in the MongoDB database"""
        # Delete the previous data
        database.delete_data(collection_name)

        default_index = None
        additional_indexes = None
        if not indexes:
            # Try to deduce the index
            if 'date' in self.df.columns:
                default_index = 'date'
                if 'autonomous_region' in self.df.columns:
                    additional_indexes = ['autonomous_region']
            elif 'autonomous_region' in self.df.columns:
                default_index = 'autonomous_region'
        else:
            # Use the provided indexes
            default_index = indexes[0]
            if len(indexes) > 1:
                additional_indexes = indexes[1:]

        if not self.mongo_data:
            self.mongo_data = self.df.to_dict('records')

        database.store_data(collection_name, self.mongo_data, additional_indexes=additional_indexes,
                            default_index=default_index)


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
