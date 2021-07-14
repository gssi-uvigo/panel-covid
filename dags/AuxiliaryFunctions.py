import math
import os
import re
from abc import abstractmethod

import PyPDF2
import requests
from pymongo import ASCENDING, DESCENDING
import pandas as pd

from airflow.providers.mongo.hooks.mongo import MongoHook


class MongoDatabase:
    """
        Instance of the MongoDB database where all the extracted data will be stored.
        The database URI must be set in the "Connections" of Apache Airflow.
    """

    collection_indexes = {
        'default': [('date', DESCENDING), ('autonomous_region', ASCENDING)],  # this is the most common index
        'clinic_description': [('type', ASCENDING), ('description', ASCENDING)],
        'population_ar': [('autonomous_region', ASCENDING)],
        'death_causes': [('death_cause', ASCENDING), ('age_range', ASCENDING)],
        'chronic_illnesses': [('illness', ASCENDING)],
        'outbreaks_description': [('date', DESCENDING), ('scope', ASCENDING), ('subscope', ASCENDING)],
        'top_death_causes': [('death_cause', ASCENDING)]
    }

    extracted_db_name = 'covid_extracted_data'
    analyzed_db_name = 'covid_analyzed_data'

    def __init__(self, database_name):
        """
            Connect to the database.
        """
        self.client = MongoHook(conn_id='mongo_covid').get_conn()
        self.db = self.client.get_database(database_name)

    @staticmethod
    def create_collection_index(collection):
        """Create a custom index for a collection, to improve I/O tasks"""
        if len(list(collection.list_indexes())) < 2:
            # Not index created yet, let's create it
            if collection.name in MongoDatabase.collection_indexes:
                index = MongoDatabase.collection_indexes[collection.name]
            else:
                index = MongoDatabase.collection_indexes['default']

            collection.create_index(index)

    def read_data(self, collection_name, filters=None, projection=None):
        """
            Read data from the database and return it as a DataFrame.
            :param collection_name: Name of the collection from which the data will be read
            :param filters: (optional) Dictionary with the query filters.
            :param projection: (optional) List of columns to retrieve.
        """
        collection = self.db.get_collection(collection_name)
        if projection:
            projected_fields = {field: 1 for field in projection}
        else:
            projected_fields = {}

        projected_fields['_id'] = 0

        query = collection.find(filters, projected_fields)
        df = pd.DataFrame(query)

        return df

    def store_data(self, collection_name, data, overwrite=True):
        """
            Store data in the database.
            :param collection_name: Name of the collection in which the data will be stored
            :param data: document or documents to be stored in the collection
            :param overwrite: whether to delete the previous data in the collection before storing the new one
        """

        collection = self.db.get_collection(collection_name)

        if overwrite:
            collection.delete_many({})

        MongoDatabase.create_collection_index(collection)

        if type(data) == list:
            # Several documents to be inserted
            collection.insert_many(data)
        elif type(data) == dict:
            # One single document to be inserted
            collection.insert_one(data)

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
    age_range_translations = {'\*': '', ' *[\(\)] *': '', 'Todas las edades': 'total', 'Total': 'total',
                              'Menores de ([0-9]*) año': '0-\\1', 'De ([0-9]*) a ([0-9]*) años': '\\1-\\2',
                              'De ([0-9]*) años y más': '≥\\1', '([0-9]*) y más años': '≥\\1'}

    def __init__(self, file, separator=',', decimal='.', thousands=',', dataframe=None, encoding=None):
        self.df = dataframe if isinstance(dataframe, pd.DataFrame) else \
            pd.read_csv(file, sep=separator, decimal=decimal, thousands=thousands, encoding=encoding)
        self.mongo_data = None
        self.__process_dataset__()

    @abstractmethod
    def __process_dataset__(self):
        """Transform the DataFrame to extract the important information"""

    def store_dataset(self, database, collection_name):
        """Store the dataset in the MongoDB database"""

        if not self.mongo_data:
            self.mongo_data = self.df.to_dict('records')

        database.store_data(collection_name, self.mongo_data)


class PDFReport:
    """Represent a report in PDF"""
    autonomous_regions = ['Andalucía', 'Aragón', 'Asturias', 'Baleares', 'Canarias', 'Cantabria', 'Castilla_La_Mancha',
                          'Castilla_y_León', 'Cataluña', 'Ceuta', 'Comunidad_Valenciana', 'Extremadura', 'Galicia',
                          'Madrid', 'Melilla', 'Murcia', 'Navarra', 'País_Vasco', 'La_Rioja']

    def __init__(self, directory, filename):
        self.index = int(filename[:-4])
        with open(directory + '/' + filename, 'rb') as file:
            # Open the file with PyPDF
            reader = PyPDF2.PdfFileReader(file)

            # Read the pages
            pages_number = range(0, reader.getNumPages())
            pages = [reader.getPage(x).extractText() for x in pages_number]
            pages = [' '.join(x.replace('\n', '').split()).strip() for x in pages]  # remove random line breaks
            self.pages = pages

            # Extract the report date (this will depend on the report type)
            self.date = self.__extract_date__(reader)

            self.tables_index_numbers = {}  # for each table number save the page number
            self.tables_index_names = {}  # for each table number, save the name of that table

            # Get the page where is each table
            for page_number, page_content in enumerate(pages):
                found_table_titles = re.findall('Tabla [0-9]+[a-z]?[.,]', page_content)
                for title in found_table_titles:
                    table_number = int(re.match('Tabla [0-9]+', title).group()[6:])
                    self.tables_index_numbers[table_number] = page_number

    @staticmethod
    def get_real_autonomous_region_name(ar):
        """
            Transform a slashed (for example, Castilla_y_León) Autonomous Region name into its original (Castilla y
            León)
        """
        return ar.replace('_', ' ')

    @classmethod
    def process_reports_batch(cls, directory, last_index):
        """Return a list with a report object for each PDF report in the specified directory"""
        reports = []
        file_list = list(filter(lambda x: x.endswith('.pdf') and int(x[:-4]) > last_index, os.listdir(directory)))
        new_last_index = last_index
        for number, filename in enumerate(file_list):
            # Update the progress percentage
            percentage = 100 * (number / len(file_list))
            print("Reading progress: %.2f%%" % percentage, end="\r", flush=True)

            new_report = cls(directory, filename)
            reports.append(new_report)
            new_last_index = max(new_last_index, new_report.index)

        print("Reading progress: 100%", end="\r", flush=True)

        return reports, new_last_index

    @abstractmethod
    def __extract_date__(self, reader):
        """Extract the date when the report was written"""

    @staticmethod
    def convert_value_to_number(value, is_float=False):
        """Convert a string to an int or float. If the provided string is not a number, it will return None."""
        if value[-1] == '%':
            value = value[:-1]

        if is_float:
            value = value.replace(',', '.')
            try:
                number = float(value)
            except ValueError:
                number = None
        else:
            value = value.replace('.', '')
            try:
                number = int(value)
            except ValueError:
                number = None

        return number

    def get_table_page_by_name(self, table_name):
        """Return the text of the report page containing the specified table"""
        if table_name in self.tables_index_names:
            table_number = self.tables_index_names[table_name]
            table_pagenumber = self.tables_index_numbers[table_number]
            table_page = self.pages[table_pagenumber]
            return table_page, table_number
        else:
            # The report doesn't contain the requested table
            return None, None

    def get_table_page_by_number(self, table_number):
        """Return the text of the report page containing the specified table"""
        if table_number in self.tables_index_numbers:
            page_number = self.tables_index_numbers[table_number]
            return self.pages[page_number]
        else:
            # The report doesn't contain the requested table
            return None

    @staticmethod
    def remove_ar_spaces_and_symbols(page, table_number):
        """Replace the spaces in the Autnomous Regions names with _ and remove undesired symbols like *, %..."""
        replaced_page = page.replace('Castilla La Mancha', 'Castilla_La_Mancha') \
            .replace('Castilla-La Mancha', 'Castilla_La_Mancha') \
            .replace('C. Valenciana', 'Comunidad_Valenciana') \
            .replace('C Valenciana', 'Comunidad_Valenciana') \
            .replace('País Vasco', 'País_Vasco') \
            .replace('Castilla y León', 'Castilla_y_León') \
            .replace('La Rioja', 'La_Rioja') \
            .replace('Total general', 'ESPAÑA') \
            .replace('Total', 'ESPAÑA') \
            .replace('Islas Baleares', 'Baleares') \
            .replace('Islas Canarias', 'Canarias')

        if table_number:
            # The "Tabla X" expression is used later for trimming the table data in the text array
            replaced_page = replaced_page.replace(f'Tabla {table_number}.', f'_TABLA{table_number}_')

        replaced_page = re.sub('[^A-zÀ-ú0-9,.<>≤≥ \-]', '', replaced_page)  # remove useless symbols, like %, *, (, etc.
        return replaced_page

    def get_table_position(self, table_number):
        """
            Detect if there is another table in the same page, in order to allow the selection of the proper data.
            :return: 0 if there is not another table, 1 if there is one to the left, -1 if there is one right.
        """
        table_page_number = self.tables_index_numbers[table_number]

        if table_number + 1 in self.tables_index_numbers and self.tables_index_numbers[table_number + 1] \
                == table_page_number:
            # Another table after/to the right
            return -1
        elif table_number - 1 in self.tables_index_numbers and self.tables_index_numbers[table_number - 1] \
                == table_page_number:
            # Another table before/to the left
            return 1
        else:
            # No more tables
            return 0

    @staticmethod
    def extract_table_from_page(page, table_number, table_position, header_column=None):
        """
            Extract a table from a page, returning it as a list where each row is represented as a list item, containing
            all the columns.
        """
        if header_column is None:
            header_column = PDFReport.autonomous_regions
        header_cell_regex = '[A-zÀ-ú_]'
        table_page = page.split()

        table_start = table_page.index(f'_TABLA{table_number}_')  # get the beginning of the columns row
        first_word = header_column[0]
        table_first_row = table_page.index(first_word, table_start)  # get the beginning of the first row

        table = []
        row = []
        for i, cell in enumerate(table_page[table_first_row:]):
            if re.match(header_cell_regex, cell):
                # New row?
                if cell in header_column:  # New row?
                    row = []
                    table.append(row)
                    row.append(cell)
                else:
                    # The table has finished
                    break
            else:
                # New cell of the current row
                row.append(cell)

        # Does the array table contain only our table, or another table in the same page?
        if table_position == 0:
            # Only our table: return the table as an array where each row is represented as a list item,
            # containing all the columns
            return table
        else:
            # There are two tables in the table variable: we need to extract the data corresponding to our table
            if table_position == 1:
                # Our table is in the right/down side: take only the even rows
                return table[1::2]
            elif table_position == -1:
                # Our table is in the left/upper side: take only the odd rows
                return table[::2]

    @staticmethod
    def get_number_of_samples(percentage, total):
        """Get the size of a sample of which we know the number of items a specific percentage represents."""
        return math.floor(total / (percentage / 100)) if percentage > 0 else None

    @staticmethod
    def extract_numeric_range(numeric_range):
        """Extract the numeric range from a string with (a-b) format"""
        if re.search("[0-9]+-[0-9]+", numeric_range):
            lower_range = int(re.search("[0-9]+.*-", numeric_range).group()[:-1])
            higher_range = int(re.search("-.*[0-9]+", numeric_range).group()[1:])
        else:
            lower_range = None
            higher_range = None
        return lower_range, higher_range


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
