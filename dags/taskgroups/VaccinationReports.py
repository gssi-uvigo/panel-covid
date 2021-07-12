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
        vaccination_data = []
        vaccination_single = []
        vaccination_complete = []

        for file in os.listdir(VaccinationReportsTaskGroup.reports_folder):
            # Read the report
            df = pd.read_excel(VaccinationReportsTaskGroup.reports_folder + '/' + file, sheet_name=None)

            # Get the report date
            date_string = file[-12:-4]
            date_report = dt.strptime(date_string, VaccinationReportsTaskGroup.date_filename_format)
            print(f"Reading report of {date_report.isoformat()}")

            # Get the basic vaccination data
            df_basic_data = df[list(df.keys())[0]]

            # Translate the DataFrame columns
            columns_translations = {'Unnamed: 0': 'autonomous_region', 'Dosis entregadas (1)': 'received_doses.total',
                                    'Total Dosis entregadas (1)': 'received_doses.total',
                                    'Dosis entregadas Pfizer (1)': 'received_doses.Pfizer',
                                    'Dosis entregadas Moderna (1)': 'received_doses.Moderna',
                                    'Dosis entregadas AstraZeneca (1)': 'received_doses.AstraZeneca',
                                    'Dosis entregadas Janssen (1)': 'received_doses.Janssen',
                                    'Dosis administradas (2)': 'applied_doses',
                                    '% sobre entregadas': 'percentage_applied_doses',
                                    'Nº Personas con al menos 1 dosis': 'number_at_least_single_dose_people',
                                    'Nº Personas vacunadas(pauta completada)': 'number_fully_vaccinated_people',
                                    'Fecha de la última vacuna registrada (2)': 'date'}
            df_basic_data = df_basic_data.rename(columns=columns_translations)
            df_basic_data['autonomous_region'] = df_basic_data['autonomous_region'].replace({'Totales': 'España'})

            # Transform some columns
            df_basic_data['date'] = date_report
            df_basic_data['percentage_applied_doses'] = 100 * df_basic_data['percentage_applied_doses']

            # Save into MongoDB
            df_dict = df_basic_data.to_dict('records')
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

            vaccination_data.extend(mongo_data)

            if len(df) > 3:
                # Newer vaccination reports: get the age ranges
                df_single_dose = df[list(df.keys())[-2]]
                df_complete_dose = df[list(df.keys())[-1]]

                for number_doses in ['single', 'complete']:
                    df_doses = df_single_dose if number_doses == 'single' else df_complete_dose

                    # Remove useless columns and rename the useful ones
                    columns = df_doses.columns

                    if any(['20-29' in col for col in df_doses.columns]):
                        # From the last week of June, the reports age range has changed to 12-19, 20-29, 30-39, 40-49...
                        columns_translations = {'Unnamed: 0': 'autonomous_region', '%': '80+', '%.1': '70-79',
                                                '%.2': '60-69', '%.3': '50-59', '%.4': '40-49', '%.5': '30-39',
                                                '%.6': '20-29', '%.7': '12-19', columns[-1]: 'total'}
                    else:
                        # Before, it used to be 16-17, 18-24, 25-49 and then 50-59, 60-69...
                        columns_translations = {'Unnamed: 0': 'autonomous_region', '%': '80+', '%.1': '70-79',
                                                '%.2': '60-69', '%.3': '50-59', '%.4': '25-49', '%.5': '18-24',
                                                '%.6': '16-17', columns[-1]: 'total'}

                    if len(columns) > 20:
                        # For each age range, the sheet contains the number of vaccinated people, the total population
                        # and the percentage
                        df_doses = df_doses.drop(
                            columns=[columns[i] for i in [1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19, 20, 22, 23]])
                    else:
                        # For each age range, the sheet contains the number of vaccinated people and the percentage
                        df_doses = df_doses.drop(
                            columns=[columns[i] for i in [1, 3, 5, 7, 9, 11, 13, 15, 17, 18, 19]])

                    df_doses = df_doses.rename(columns=columns_translations)
                    df_doses['autonomous_region'] = df_doses['autonomous_region'].replace({'Total España': 'España'})

                    # Remove information about the navy
                    df_doses = df_doses[df_doses['autonomous_region'] != 'Fuerzas Armadas']

                    # Remove invalid data
                    df_doses = df_doses.dropna()

                    # Trim the autonomous region name (some have a trailing space for unknown reason)
                    df_doses['autonomous_region'] = df_doses['autonomous_region'].apply(lambda x: x.strip())

                    # Multiply by 100 the percentages
                    df_doses[df_doses.columns[1:]] = 100 * df_doses[df_doses.columns[1:]]

                    # Add the date to the DataFrame
                    df_doses['date'] = date_report

                    # Melt age range columns
                    df_doses = df_doses.melt(id_vars=['autonomous_region', 'date'], var_name='age_range',
                                             value_name='percentage')

                    # Convert data to dict
                    df_dict = df_doses.to_dict('records')

                    if number_doses == 'single':
                        vaccination_single.extend(df_dict)
                    else:
                        vaccination_complete.extend(df_dict)

        # Store the data in MongoDB
        database = MongoDatabase(MongoDatabase.extracted_db_name)
        database.store_data("vaccination_general", vaccination_data)
        database.store_data("vaccination_ages_single", vaccination_single)
        database.store_data("vaccination_ages_complete", vaccination_complete)

