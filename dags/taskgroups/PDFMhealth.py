"""
    Download the reports from the Ministry of Health, extract the data from the PDFs, and store it in the database.
"""

import os
import pickle
import re
import requests
from datetime import datetime as dt, timedelta as td

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from AuxiliaryFunctions import PDFReport, MongoDatabase


class MHealthPDFReport(PDFReport):
    """Represent a Ministry of Health report"""

    def __init__(self, directory, filename):
        super().__init__(directory, filename)

        # Extract the tables names
        self.__extract_tables_names_index__()

    def __extract_date__(self, reader):
        date_string = reader.documentInfo['/CreationDate'][2:10]
        date_object = dt(int(date_string[0:4]), int(date_string[4:6]), int(date_string[6:8]))
        return date_object

    def __extract_tables_names_index__(self):
        diagnostic_tests_regex = 'Tabla [1-9]+\. Total de .+ realizad'
        hospital_pressure_regex = 'Tabla [1-9]+\. Situación capacidad asistencial'
        hospital_cases_regex = 'Tabla [1-9]+\. Casos (de )?COVID-19.+, ingreso en UCI'
        outbreaks_description_regex = 'Tabla [0-9]+\. Distribución del nº de brotes y casos por ámbito'

        regex_list = {'diagnostic_tests': diagnostic_tests_regex, 'hospital_pressure': hospital_pressure_regex,
                      'hospital_cases': hospital_cases_regex,
                      'outbreaks_description': outbreaks_description_regex}

        for table_name, regex in regex_list.items():
            for page in self.pages:
                match_result = re.search(regex, page)
                if match_result:  # one table of interest was found
                    table_number = int(re.match('Tabla [0-9]+', match_result.group()).group()[6:])
                    self.tables_index_names[table_name] = int(table_number)
                    break

    def get_hospital_pressure(self):
        """Return the hospital pressure data for this report or None if it's not available in this report"""
        hospital_pressure_report = []

        table_page, table_number = self.get_table_page_by_name('hospital_pressure')
        if table_page:
            # This data is only available from the report number 189
            ic_beds_percentage_included = '% Camas Ocupadas UCI COVID' in table_page

            table_page = PDFReport.remove_ar_spaces_and_symbols(table_page, table_number)
            table_position = self.get_table_position(table_number)
            table = PDFReport.extract_table_from_page(table_page, table_number, table_position)

            for row in table:
                ar = row[0]

                ar_hospital_admissions = PDFReport.convert_value_to_number(row[-2])
                ar_hospital_discharges = PDFReport.convert_value_to_number(row[-1])

                if ic_beds_percentage_included:
                    # Columns: AR, hospitalized patients, % beds, IC patients, % IC beds, admissions, discharges
                    ar_patients_hospital = PDFReport.convert_value_to_number(row[1])
                    ar_beds_percentage = PDFReport.convert_value_to_number(row[-5], is_float=True)
                    ar_patients_ic = PDFReport.convert_value_to_number(row[-4])
                    ar_ic_beds_percentage = PDFReport.convert_value_to_number(row[-3], is_float=True)
                    pressure_ar = {
                        'hospitalized_patients': ar_patients_hospital,
                        'beds_percentage': ar_beds_percentage, 'ic_patients': ar_patients_ic,
                        'ic_beds_percentage': ar_ic_beds_percentage,
                        'admissions': ar_hospital_admissions, 'discharges': ar_hospital_discharges
                    }

                else:
                    # Columns: AR, hospitalized patients, IC patients, % beds, admissions, discharges
                    ar_patients_hospital = PDFReport.convert_value_to_number(row[1])
                    ar_beds_percentage = PDFReport.convert_value_to_number(row[-3], is_float=True)
                    ar_patients_ic = PDFReport.convert_value_to_number(row[-4])
                    pressure_ar = {
                        'hospitalized_patients': ar_patients_hospital,
                        'beds_percentage': ar_beds_percentage, 'ic_patients': ar_patients_ic,
                        'admissions': ar_hospital_admissions, 'discharges': ar_hospital_discharges
                    }

                hospital_pressure_report.append(
                    {'date': self.date - td(days=1), 'autonomous_region': PDFReport.get_real_autonomous_region_name(ar),
                     'pressure': pressure_ar}
                )

            return hospital_pressure_report

    def get_diagnostic_tests(self):
        """Return the diagnostic tests data for this report or None if it's not available in this report"""

        diagnostic_tests_report = []

        table_page, table_number = self.get_table_page_by_name('diagnostic_tests')
        if table_page:
            # This data is only available from the report number 189
            table_page = PDFReport.remove_ar_spaces_and_symbols(table_page, table_number)
            table_position = self.get_table_position(table_number)
            table = PDFReport.extract_table_from_page(table_page, table_number, table_position)

            positivity_included = 'Positividad' in table_page
            rate_included = 'Tasa' in table_page

            for row in table:
                ar = row[0]
                total_tests = None
                positivity = None

                # Number of diagnostics
                if rate_included:
                    total_tests_column = -3
                else:
                    total_tests_column = -1

                ar_total_tests = row[total_tests_column]
                total_tests = PDFReport.convert_value_to_number(ar_total_tests)

                # Positivity
                if positivity_included:
                    positivity_column = -1
                    ar_positivity = row[positivity_column]
                    positivity = PDFReport.convert_value_to_number(ar_positivity, is_float=True)

                diagnostic_tests_report.append(
                    {'date': self.date - td(days=3), 'autonomous_region': PDFReport.get_real_autonomous_region_name(ar),
                     'total_diagnostic_tests': total_tests, 'positivity': positivity})

            return diagnostic_tests_report

    def get_outbreaks_description(self):
        """
            Return the outbreaks data for this report or None if it's not available in this report.

            This table is very special, that's why the standard table extracting methods used in other functions are not
            used here, hence requiring more lines of ad-hoc code.
        """
        outbreak_scopes = ['Centro_educativo', 'Centro_sanitario', 'Centro_sociosanitario',
                           'Colectivos_socialmente_vulnerables', 'Familiar', 'Mixto', 'Laboral', 'Social', 'Otro',
                           'Total']

        outbreaks_description_report = []
        if 'outbreaks_description' in self.tables_index_names and self.tables_index_names['outbreaks_description'] \
                in self.tables_index_numbers:

            table_number = self.tables_index_names['outbreaks_description']
            table_pagenumber = self.tables_index_numbers[table_number]
            table_page_1 = self.pages[table_pagenumber]
            table_page_2 = self.pages[table_pagenumber + 1]  # this table is displayed in two pages

            # Slice the tables
            table_page_1 = table_page_1[table_page_1.rfind('Casos/brote') + len('Casos/brote '):]
            table_page_2 = table_page_2[table_page_2.rfind('Casos/brote') + len('Casos/brote '):]

            table_page = table_page_1 + ' ' + table_page_2

            # Remove information between parenthesis and notes at the end of the page, and put together the separated
            # words compounding one sentence
            replacements = [
                ('\([A-zÀ-ú, \.]+\)', ''),
                (', etc.', ''),
                (', ', '/'),
                (' y/o ', '/'),
                ('1 A efectos de notificación .* de un mismo domicilio.', ''),
                ('([A-zÀ-ú]) ([A-zÀ-ú])', '\\1_\\2'),
                ('([A-zÀ-ú]) ([A-zÀ-ú])', '\\1_\\2')
            ]
            for regex, replace in replacements:
                table_page = re.sub(regex, replace, table_page)

            # Split the tables
            table = table_page.split()

            # Change the last "Otros" scope to "Otro", to distinguish it from "Otros" sub-scopes
            other_count = 0
            for word_index in reversed(range(0, len(table))):
                if table[word_index] == 'Otros':
                    other_count += 1
                    if other_count == 2:
                        table[word_index] = 'Otro'
                        break

            # Get all the row keys
            row_keys = list(filter(lambda x: any(c.isalpha() for c in x), table))
            scope = ''
            for i in range(0, len(row_keys)):
                current_key = row_keys[i]
                if current_key == "Centro_sanitario" and scope == "Laboral":
                    # The extraction would fail, because there is a scope called "Centro sanitario" before "Laboral"
                    continue

                if current_key in outbreak_scopes:
                    scope = current_key

                # Iterate row by row
                if i < len(row_keys) - 1:
                    row = table[
                          table.index(current_key, table.index(scope)) + 1:table.index(row_keys[i + 1],
                                                                                       table.index(scope))]
                else:
                    row = table[table.index(current_key, table.index(scope)) + 1:]

                if len(row) == 6:
                    # 6 columns: accumulated outbreaks, accumulated cases, accumulated cases/outbreak, new oubreaks,
                    # new cases, new cases/outbreak We will select only the accumulated outbreaks, cases and ratio
                    accumulated_outbreaks_number = int(row[0].replace('.', ''))
                    accumulated_cases_number = int(row[1].replace('.', ''))
                elif len(row) == 10:
                    # 10 columns: only the accumulated number of outbreaks and of cases will be selected
                    accumulated_outbreaks_number = int(row[0].replace('.', ''))
                    accumulated_cases_number = int(row[2].replace('.', ''))
                else:
                    continue

                accumulated_cases_per_outbreak = accumulated_cases_number / accumulated_outbreaks_number
                if scope == current_key:
                    current_key = 'Total'

                outbreaks_description_report.append({
                    'date': self.date - td(days=1),
                    'scope': scope,
                    'subscope': current_key,
                    'outbreaks': {
                        'number': accumulated_outbreaks_number,
                        'cases': accumulated_cases_number,
                        'cases_per_outbreak': accumulated_cases_per_outbreak
                    }})

            return outbreaks_description_report

    def get_hospitalized_cases(self):
        """Return the hospitalized cases data for this report or None if it's not available in this report"""
        hospitalized_cases_report = []

        table_page, table_number = self.get_table_page_by_name('hospital_cases')
        if table_page:
            table_page = PDFReport.remove_ar_spaces_and_symbols(table_page, table_number)
            table_position = self.get_table_position(table_number)
            table = PDFReport.extract_table_from_page(table_page, table_number, table_position)
            table_expected_width = max([len(row) for row in table])  # this table can have sometimes the new cases
            # columns empty, so this will be used for knowing if a row has empty values

            for row in table:
                ar = row[0]

                # Number of hospitalized cases
                if 'IA' in table_page:
                    # Total hospitalized cases: 4th column, total IC cases: 5th column
                    total_hospitalized_column = 3
                    total_ic_column = 4

                else:
                    # Total hospitalized cases: 2nd column, total IC cases: 4th column
                    total_hospitalized_column = 1
                    total_ic_column = 3

                if len(row) == table_expected_width - 1:
                    # New IC cases missing for this row
                    total_ic_column = None
                elif len(row) == table_expected_width - 2:
                    # New hospital and IC cases missing for this row: no data of interest for this row
                    continue

                total_hospitalized_cases = PDFReport.convert_value_to_number(row[total_hospitalized_column])
                total_ic_cases = PDFReport.convert_value_to_number(row[total_ic_column]) if total_ic_column else None

                hospitalized_cases_report.append({
                    'date': self.date - td(days=1),
                    'autonomous_region': PDFReport.get_real_autonomous_region_name(ar),
                    'hospitalizations': {
                        'total_hospitalized_cases': total_hospitalized_cases,
                        'total_ic_cases': total_ic_cases
                    }
                })

            return hospitalized_cases_report


class PDFMhealthTaskGroup(TaskGroup):
    """
        TaskGroup that downloads the reports from the Ministry of Health,
        extracts the data, and stores it into the database
    """

    reports_directory = 'mhealth_reports'
    processed_reports_directory = reports_directory + '/processed'
    mongo_collection_name = 'covid_extracted_data'

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(PDFMhealthTaskGroup, self) \
            .__init__("mhealth_reports",
                      tooltip="Download, extract and store the data from the Ministry of Health PDF reports",
                      dag=dag)

        # Instantiate the operators
        download_op = PythonOperator(task_id='download_mhealth_reports',
                                     python_callable=PDFMhealthTaskGroup.download_mhealth_reports,
                                     task_group=self,
                                     dag=dag)

        process_op = PythonOperator(task_id='process_mhealth_reports',
                                    python_callable=PDFMhealthTaskGroup.process_pdfs,
                                    task_group=self,
                                    dag=dag)

        extract_op = PythonOperator(task_id='mhealth_extract_and_store',
                                    python_callable=PDFMhealthTaskGroup.extract_and_store,
                                    task_group=self,
                                    dag=dag)

        download_op >> process_op >> extract_op

    @staticmethod
    def download_mhealth_reports():
        """Download all the PDF reports released by the Ministry of Health"""
        url = 'https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Actualizacion_' \
              '{index}_COVID-19.pdf'

        report_number = 55  # the first report to work with will be the number 55 (25th March 2020)

        if PDFMhealthTaskGroup.reports_directory not in os.listdir():
            os.mkdir(PDFMhealthTaskGroup.reports_directory)  # create the folder for downloading the reports

        while True:
            # Download report by report, until there are no more available
            if '{}.pdf'.format(report_number) not in os.listdir(PDFMhealthTaskGroup.reports_directory):
                # Just download the report if it hasn't been downloaded yet
                print("Downloading report %i" % report_number)
                request = requests.get(url.format(index=report_number))
                if request.status_code < 400:
                    with open("mhealth_reports/{number}.pdf".format(number=report_number), "wb") as file:
                        file.write(request.content)
                else:
                    # No more reports available
                    break
            report_number += 1

    @staticmethod
    def process_pdfs():
        """Process the PDF files and save the processed data"""
        file_list = list(filter(lambda x: x.endswith('.pdf'), os.listdir(PDFMhealthTaskGroup.reports_directory)))

        # If it hadn't been created, create the directory where the processed files will be stored
        os.makedirs(PDFMhealthTaskGroup.processed_reports_directory, exist_ok=True)

        for filename in file_list:
            processed_report_filename = filename[:-4] + '.bin'
            if processed_report_filename not in os.listdir(PDFMhealthTaskGroup.processed_reports_directory):
                # Process only the new reports
                print("Processing RENAVE report %s" % filename)
                try:
                    report = MHealthPDFReport(PDFMhealthTaskGroup.reports_directory, filename)
                    with open(PDFMhealthTaskGroup.processed_reports_directory + '/' + processed_report_filename, 'wb') \
                            as f:
                        # Store the processed report as a file, so it can be read later in the next task
                        pickle.dump(report, f)
                except Exception:
                    # Error processing the report
                    print("Error processing the report %i. Skipping it." % filename)

    @staticmethod
    def extract_and_store():
        """Read the processed PDF files, extract the information, and store it into the database"""
        reports = []
        documents_diagnostic_tests = []
        documents_hospitals_pressure = []
        documents_outbreaks_description = []
        database = MongoDatabase(MongoDatabase.extracted_db_name)

        # Read the processed reports
        processed_reports_files = os.listdir(PDFMhealthTaskGroup.processed_reports_directory)
        for file in processed_reports_files:
            with open(PDFMhealthTaskGroup.processed_reports_directory + '/' + file, 'rb') as f:
                processed_report = pickle.load(f)
                reports.append(processed_report)

        for report in reports:
            try:
                diagnostic_tests = report.get_diagnostic_tests()
                if diagnostic_tests:
                    documents_diagnostic_tests.extend(diagnostic_tests)
            except Exception:
                print("Error trying to extract the diagnostic tests data from Health Ministry report %i" % report.index)

            try:
                hospitals_pressure = report.get_hospital_pressure()
                if hospitals_pressure:
                    documents_hospitals_pressure.extend(hospitals_pressure)
            except Exception:
                print("Error trying to extract the hospital pressure from Health Ministry report %i" % report.index)

            try:
                outbreaks_description = report.get_outbreaks_description()
                if outbreaks_description:
                    documents_outbreaks_description.extend(outbreaks_description)
            except Exception:
                print("Error trying to extract the transmission indicators from RENAVE report %i" % report.index)

        database.store_data('diagnostic_tests', documents_diagnostic_tests)
        database.store_data('hospitals_pressure', documents_hospitals_pressure)
        database.store_data('outbreaks_description', documents_outbreaks_description)
