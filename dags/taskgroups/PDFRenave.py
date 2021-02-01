"""
    Download the RENAVE reports, extract the data from the PDFs, and store it in the database.
"""
import locale
import os
import pickle
import re
import requests
from datetime import datetime as dt

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup

from AuxiliaryFunctions import PDFReport, MongoDatabase


class RenavePDFfReport(PDFReport):
    """Represent a RENAVE report"""

    def __extract_date__(self, reader):
        # Change the locale to the Spanish one
        locale.setlocale(locale.LC_ALL, 'es_ES')

        text = reader.getPage(0).extractText()
        text = ' '.join(text.replace('\n', '').split()).strip()
        reference_sentences = {'Fecha del informe: ': 0,
                               'Situación de COVID-19 en España a ': 0,
                               'Informe COVID-2019 nº ': 3,
                               'Informe SARS-CoV-2 nº ': 3,
                               'Informe COVID-19 nº ': 3,
                               'Informe COVID–19 . ': 0
                               }

        for reference, number_of_spaces in reference_sentences.items():
            if reference in text:
                pos_init = text.find(reference) + len(reference) + number_of_spaces
                pos_fin = text.find('202', pos_init) + 4
                report_date = text[pos_init:pos_fin].strip()
                try:
                    report_date = dt.strptime(report_date, '%d de %B de %Y')
                except ValueError:
                    try:
                        report_date = dt.strptime(report_date, '%d %B de %Y')
                    except ValueError:
                        report_date = dt.strptime(report_date, '%d-%m-%Y')
                    finally:
                        locale.setlocale(locale.LC_ALL, 'en_US')

                locale.setlocale(locale.LC_ALL, 'en_US')
                return report_date

    def get_clinic_description(self):
        """
            Return the clinic description data for this report or None if it's not available in this report.

            This table is very special, that's why the standard table extracting methods used in other functions are not
            used here, hence requiring more lines of ad-hoc code.
        """
        symptoms_list = {
            'fiebre o reciente historia de fiebre': 'fever', 'tos': 'cough', 'dolor de garganta': 'sore_throat',
            'disnea': 'dyspnoea', 'escalofríios': 'chill', 'vómitos': 'vomit', 'diarrea': 'dhiarrea',
            'neumonía (rx o clínica)': 'pneumonia', 'neumonía (radiológica o clínica)': 'pneumonia', 'sdra': 'ards',
            'síndrome de distrés respiratorio agudo': 'ards', 'otros síntomas resp.': 'other_respiratory',
            'fallo renal agudo': 'aki', 'otros síntomas': 'others'
        }

        diseases_list = {
            'una o más': 'one_or_more', 'enfermedad cardiaca': 'cardiac', 'enfermedad cardiovascular': 'cardiac',
            'enfermedad respiratoria': 'respiratory', 'diabetes': 'diabetes', 'inmunodepresión': 'immunosuppression',
            'enfermedad neuromuscular': 'neuromuscular', 'enfermedad hepática': 'liver', 'enfermedad renal': 'renal',
            'cáncer': 'cancer', 'hipertensión arterial': 'hypertension', 'otra': 'other'
        }

        table_number = 2  # the table number in RENAVE reports is always the same!

        clinic_description_report = []

        if self.index in range(16, 34):
            # Data only available from report number 12 to 33; reports from 12 to 15 are illegible in that page
            clinic_page = self.get_table_page_by_number(table_number)
            clinic_page = re.sub('[^A-zÀ-ú0-9,. \-<>]', '', clinic_page).lower()
            clinic_page = clinic_page.replace('enfermedad de base y factores de riesgo',
                                              'enfermedades_previas').replace(
                'enfermedades y factores de riesgo', 'enfermedades_previas')

            # Replace the name of the symptoms and the previous diseases
            for before, after in symptoms_list.items():
                clinic_page = clinic_page.replace(' ' + before + ' ', ' ' + after + ' ')

            for before, after in diseases_list.items():
                clinic_page = clinic_page.replace(' ' + before + ' ', ' ' + after + ' ')

            clinic_table = clinic_page.split()

            # Sometimes women column is before men column, other times after, we need to know
            index_men = clinic_table.index('hombres')
            index_women = clinic_table.index('mujeres')

            # Sometimes the number of samples is included as a column, other times no, we need to know
            is_samples_number_column = clinic_table[clinic_table.index('total') - 1] != 'características'

            # Symptoms table
            table_symptoms_start = clinic_table.index('síntomas')
            table_symptoms_end = clinic_table.index('enfermedades_previas', table_symptoms_start)
            symptoms_table = clinic_table[table_symptoms_start + 1:table_symptoms_end]

            # Previous diseases table
            table_previous_diseases_start = clinic_table.index('enfermedades_previas')
            table_previous_diseases_end = clinic_table.index('hospitalización', table_previous_diseases_start)
            diseases_table = clinic_table[table_previous_diseases_start + 1:table_previous_diseases_end]

            # Look for symptoms
            for symptom in set(symptoms_list.values()):
                if symptom in symptoms_table:
                    row = symptoms_table.index(symptom) + (1 if is_samples_number_column else 0)

                    # Number of patients
                    number_of_patients_total = PDFReport.convert_value_to_number(symptoms_table[row + 1])

                    # Number of patients: percentage
                    number_of_patients_percentage = PDFReport.convert_value_to_number(symptoms_table[row + 2],
                                                                                      is_float=True)

                    # Number of women
                    number_of_women = PDFReport.convert_value_to_number(
                        symptoms_table[row + (3 if index_women < index_men else 5)])

                    # Number of women: percentage
                    number_of_women_percentage = PDFReport.convert_value_to_number(
                        symptoms_table[row + (4 if index_women < index_men else 6)], is_float=True)

                    # Number of men
                    number_of_men = PDFReport.convert_value_to_number(
                        symptoms_table[row + (5 if index_women < index_men else 3)])

                    # Number of men: percentage
                    number_of_men_percentage = PDFReport.convert_value_to_number(
                        symptoms_table[row + (6 if index_women < index_men else 4)], is_float=True)

                    # Number of samples = number of patients / percentage
                    number_of_samples_total = PDFReport.get_number_of_samples(number_of_patients_percentage,
                                                                              number_of_patients_total)
                    number_of_samples_woman = PDFReport.get_number_of_samples(number_of_women_percentage,
                                                                              number_of_women)
                    number_of_samples_men = PDFReport.get_number_of_samples(number_of_men_percentage,
                                                                            number_of_men)

                    clinic_description_report.append({
                        'date': self.date,
                        'type': 'symptom',
                        'description': symptom,
                        'patients': {
                            'total': {'samples': number_of_samples_total, 'number': number_of_patients_total,
                                      'percentage': number_of_patients_percentage},
                            'women': {'samples': number_of_samples_woman, 'number': number_of_women,
                                      'percentage': number_of_women_percentage},
                            'men': {'samples': number_of_samples_men, 'number': number_of_men,
                                    'percentage': number_of_men_percentage}
                        }
                    })

            # Look for diseases
            for disease in set(diseases_list.values()):
                if disease in diseases_table:
                    row = diseases_table.index(disease) + (1 if is_samples_number_column else 0)

                    # Number of patients
                    number_of_patients_total = PDFReport.convert_value_to_number(diseases_table[row + 1])

                    # Number of patients: percentage
                    number_of_patients_percentage = PDFReport.convert_value_to_number(diseases_table[row + 2],
                                                                                      is_float=True)

                    # Number of women
                    number_of_women = PDFReport.convert_value_to_number(
                        diseases_table[row + (3 if index_women < index_men else 5)])

                    # Number of women: percentage
                    number_of_women_percentage = PDFReport.convert_value_to_number(
                        diseases_table[row + (4 if index_women < index_men else 6)], is_float=True)

                    # Number of men
                    number_of_men = PDFReport.convert_value_to_number(
                        diseases_table[row + (5 if index_women < index_men else 3)])

                    # Number of men: percentage
                    number_of_men_percentage = PDFReport.convert_value_to_number(
                        diseases_table[row + (6 if index_women < index_men else 4)], is_float=True)

                    # Number of samples = number of patients / percentage
                    number_of_samples_total = PDFReport.get_number_of_samples(number_of_patients_percentage,
                                                                              number_of_patients_total)
                    number_of_samples_woman = PDFReport.get_number_of_samples(number_of_women_percentage,
                                                                              number_of_women)
                    number_of_samples_men = PDFReport.get_number_of_samples(number_of_men_percentage,
                                                                            number_of_men)

                    clinic_description_report.append({
                        'date': self.date,
                        'type': 'disease',
                        'description': disease,
                        'patients': {
                            'total': {'samples': number_of_samples_total, 'number': number_of_patients_total,
                                      'percentage': number_of_patients_percentage},
                            'women': {'samples': number_of_samples_woman, 'number': number_of_women,
                                      'percentage': number_of_women_percentage},
                            'men': {'samples': number_of_samples_men, 'number': number_of_men,
                                    'percentage': number_of_men_percentage}
                        }
                    })

            return clinic_description_report

    def get_transmission_indicators(self):
        """Return the transmission indicators data for this report or None if it's not available in this report"""
        transmission_indicators_report = []
        table_number = 6  # the table number in RENAVE reports is always the same!

        if self.index >= 34 and 6 in self.tables_index_numbers:  # data only available after report number 35
            # Get the table content
            table_page = self.get_table_page_by_number(table_number)
            table_page = table_page.replace("6a. ", "6. ").replace("6 a.", "6. ").replace("6, ", "6. ")
            table_page = PDFReport.remove_ar_spaces_and_symbols(table_page, table_number)

            # Extract the table
            table = PDFReport.extract_table_from_page(table_page, table_number, 0)

            # Extract the data from the table
            for row in table:
                ar = row[0]

                # Symptomatic percentage
                symptomatic_percentage = PDFReport.convert_value_to_number(row[2], is_float=True)

                # Time until diagnostic
                time_until_diagnostic_median = PDFReport.convert_value_to_number(row[5])
                time_until_diagnostic_iq = row[6]
                time_until_diagnostic_iq_high, time_until_diagnostic_iq_low = PDFReport.extract_numeric_range(
                    time_until_diagnostic_iq)

                # Unknown contact cases
                cases_unknown_contact_total = PDFReport.convert_value_to_number(row[-4])
                cases_unknown_contact_percentage = PDFReport.convert_value_to_number(row[-3], is_float=True)

                # Identified contacts per case
                identified_contacts_per_case_median = PDFReport.convert_value_to_number(row[-2])
                identified_contacts_per_case_iq = row[-1]
                identified_contacts_per_case_iq_high, identified_contacts_per_case_iq_low = PDFReport. \
                    extract_numeric_range(identified_contacts_per_case_iq)

                transmission_indicators_report.append({
                    'date': self.date,
                    'autonomous_region': PDFReport.get_real_autonomous_region_name(ar),
                    'transmission_indicators': {
                        'cases_unknown_contact': {
                            'total': cases_unknown_contact_total,
                            'percentage': cases_unknown_contact_percentage
                        },
                        'identified_contacts_per_case': {
                            'median': identified_contacts_per_case_median,
                            'iq': (identified_contacts_per_case_iq_low, identified_contacts_per_case_iq_high)
                        },
                        'days_until_diagnostic': {
                            'median': time_until_diagnostic_median,
                            'iq': (time_until_diagnostic_iq_low, time_until_diagnostic_iq_high)
                        },
                        'asymptomatic_percentage': 100 - symptomatic_percentage
                    }
                })

            return transmission_indicators_report


class PDFRenaveTaskGroup(TaskGroup):
    """TaskGroup that downloads the RENAVE reports, extracts the data, and stores it into the database"""

    reports_directory = 'renave_reports'
    processed_reports_directory = reports_directory + '/processed'

    def __init__(self, dag):
        self.dag = dag

        # Instantiate the TaskGroup
        super(PDFRenaveTaskGroup, self) \
            .__init__("renave_reports",
                      tooltip="Download, extract and store the data from the RENAVE PDF reports",
                      dag=dag)

        # Instantiate the operators
        download_op = PythonOperator(task_id='download_renave_reports',
                                     python_callable=PDFRenaveTaskGroup.download_renave_reports,
                                     task_group=self,
                                     dag=dag)

        process_op = PythonOperator(task_id='process_renave_reports',
                                    python_callable=PDFRenaveTaskGroup.process_pdfs,
                                    task_group=self,
                                    dag=dag)

        extract_op = PythonOperator(task_id='renave_extract_and_store',
                                    python_callable=PDFRenaveTaskGroup.extract_and_store,
                                    task_group=self,
                                    dag=dag)

        download_op >> process_op >> extract_op

    @staticmethod
    def download_renave_reports():
        """Download all the PDF reports released by the RENAVE"""

        print("Downloading last reports from RENAVE...")

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
                if '{}.pdf'.format(number) not in os.listdir(PDFRenaveTaskGroup.reports_directory):
                    # Download the file only if it had not been previously downloaded
                    print("Downloading report number %i: %s" % (number, report_url.replace('%20', ' ')))
                    request = requests.get(base_url + report_url)
                    if request.status_code < 400:
                        with open("renave_reports/{number}.pdf".format(number=number), "wb") as file:
                            file.write(request.content)

    @staticmethod
    def process_pdfs():
        """Process the PDF files and save the processed data"""
        file_list = list(filter(lambda x: x.endswith('.pdf'), os.listdir(PDFRenaveTaskGroup.reports_directory)))

        # If it hadn't been created, create the directory where the processed files will be stored
        os.makedirs(PDFRenaveTaskGroup.processed_reports_directory, exist_ok=True)

        for filename in file_list:
            processed_report_filename = filename[:-4] + '.bin'
            if processed_report_filename not in os.listdir(PDFRenaveTaskGroup.processed_reports_directory):
                # Process only the new reports
                print("Processing RENAVE report %s" % filename)
                try:
                    report = RenavePDFfReport(PDFRenaveTaskGroup.reports_directory, filename)
                    with open(PDFRenaveTaskGroup.processed_reports_directory + '/' + processed_report_filename, 'wb') \
                            as f:
                        # Store the processed report as a file, so it can be read later in the next task
                        pickle.dump(report, f)
                except Exception:
                    # Error processing the report
                    print("Error processing the report %s. Skipping it." % filename)

    @staticmethod
    def extract_and_store():
        """Read the processed PDF files, extract the information, and store it into the database"""
        reports = []
        documents_clinic_description = []
        documents_transmission_indicators = []
        database = MongoDatabase(MongoDatabase.extracted_db_name)

        # Read the processed reports
        processed_reports_files = os.listdir(PDFRenaveTaskGroup.processed_reports_directory)
        for file in processed_reports_files:
            with open(PDFRenaveTaskGroup.processed_reports_directory + '/' + file, 'rb') as f:
                processed_report = pickle.load(f)
                reports.append(processed_report)

        for report in reports:
            try:
                clinic_description = report.get_clinic_description()
                if clinic_description:
                    documents_clinic_description.extend(clinic_description)
            except Exception:
                print("Error trying to extract the clinic description from RENAVE report %i" % report.index)

            try:
                transmission_indicators = report.get_transmission_indicators()
                if transmission_indicators:
                    documents_transmission_indicators.extend(transmission_indicators)
            except Exception:
                print("Error trying to extract the transmission indicators from RENAVE report %i" % report.index)

        database.store_data('clinic_description', documents_clinic_description)
        database.store_data('transmission_indicators', documents_transmission_indicators)
