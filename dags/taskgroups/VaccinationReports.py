import os
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime as dt, timedelta as td

from AuxiliaryFunctions import MongoDatabase


class VaccinationReportsTaskGroup(TaskGroup):
    """TaskGroup that downloads and stores in the database the vaccination reports released by the Ministry of Health"""

    reports_folder = 'vaccination_reports'
    date_filename_format = '%Y%m%d'

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(VaccinationReportsTaskGroup, self) \
            .__init__("vaccination_reports", tooltip="Download and store in the database all the vaccination reports",
                      dag=dag)

        # Instantiate the operators
        download_data_op = PythonOperator(task_id='download_vaccination_reports',
                                          python_callable=VaccinationReportsTaskGroup.download_vaccination_reports,
                                          task_group=self,
                                          dag=dag)

        store_data_op = PythonOperator(task_id='store_vaccination_data',
                                       python_callable=VaccinationReportsTaskGroup.store_vaccination_reports,
                                       task_group=self,
                                       dag=dag)

        download_data_op >> store_data_op

    @staticmethod
    def download_vaccination_reports():
        """Download the vaccination reports from the Ministry of Health website"""

        # Create the folder for the vaccination reports, if it didn't exist yet
        if VaccinationReportsTaskGroup.reports_folder not in os.listdir():
            os.mkdir(VaccinationReportsTaskGroup.reports_folder)

        date_current_file = dt(2021, 1, 11)
        today = dt.today()
        today = dt(today.year, today.month, today.day)
        base_url = 'https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/'
        filename_url = 'Informe_Comunicacion_{date}.ods'

        # Download all the vaccination reports
        while True:
            filename = filename_url.format(date=date_current_file.strftime(
                VaccinationReportsTaskGroup.date_filename_format))
            if filename not in os.listdir(VaccinationReportsTaskGroup.reports_folder):
                request = requests.get(
                    base_url + filename.format(date=date_current_file.strftime(
                        VaccinationReportsTaskGroup.date_filename_format)))
                if request.status_code < 400:
                    # Download the report and go for the next one
                    print(f"Downloading report {filename}")
                    with open(VaccinationReportsTaskGroup.reports_folder + '/' + filename, 'wb') as f:
                        f.write(request.content)

            date_current_file = date_current_file + td(days=1)

            if date_current_file == today:
                # No more reports available
                break

    @staticmethod
    def store_vaccination_reports():
        """Store in the database the downloaded reports"""
        for file in os.listdir(VaccinationReportsTaskGroup.reports_folder):
            # Read the report as a DataFrame
            df = pd.read_excel(VaccinationReportsTaskGroup.reports_folder + '/' + file)

            # Get the report date
            date_string = file[-12:-4]
            date_report = dt.strptime(date_string, VaccinationReportsTaskGroup.date_filename_format)

            # Translate the DataFrame columns
            columns_translations = {'Unnamed: 0': 'autonomous_region', 'Dosis entregadas (1)': 'received_doses.total',
                                    'Total Dosis entregadas (1)': 'received_doses.total',
                                    'Dosis entregadas Pfizer (1)': 'received_doses.Pfizer',
                                    'Dosis entregadas Moderna (1)': 'received_doses.Moderna',
                                    'Dosis entregadas AstraZeneca (1)': 'received_doses.AstraZeneca',
                                    'Dosis administradas (2)': 'applied_doses',
                                    '% sobre entregadas': 'percentage_applied_doses',
                                    'Nº Personas vacunadas(pauta completada)': 'number_fully_vaccinated_people',
                                    'Fecha de la última vacuna registrada (2)': 'date'}
            df = df.rename(columns=columns_translations)
            df['autonomous_region'] = df['autonomous_region'].replace({'Totales': 'España'})

            # Transform some columns
            df['date'] = date_report
            df['percentage_applied_doses'] = 100 * df['percentage_applied_doses']

            # Transform the DataFrame into a MongoDB collection of documents
            df_dict = df.to_dict('records')
            mongo_data = []

            # Transform the a.b columns into a: {b: ''}
            for record in df_dict:
                transformed_record = {}
                mongo_data.append(transformed_record)
                for k, v in record.items():
                    if '.' not in k:
                        transformed_record[k] = v
                    else:
                        key, subkey = k.split('.')
                        if key not in transformed_record:
                            transformed_record[key] = {}

                        transformed_record[key][subkey] = v

            # Store the data in MongoDB
            database = MongoDatabase(MongoDatabase.extracted_db_name)
            database.store_data("vaccination", mongo_data)
