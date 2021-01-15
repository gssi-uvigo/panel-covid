"""
    Download the reports from the Ministry of Health, extract the data from the PDFs, and store it in the database.
"""

import os
import requests

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


class PDFMhealthTaskGroup(TaskGroup):
    """
        TaskGroup that downloads the reports from the Ministry of Health,
        extracts the data, and stores it into the database
    """

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

    @staticmethod
    def download_mhealth_reports():
        """Download all the PDF reports released by the Ministry of Health"""
        url = 'https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Actualizacion_'\
              '{index}_COVID-19.pdf'

        report_number = 55  # the first report to work with will be the number 55 (25th March 2020)

        if 'mhealth_reports' not in os.listdir():
            os.mkdir('mhealth_reports')  # create the folder for downloading the reports

        while True:
            # Download report by report, until there are no more available
            if '{}.pdf'.format(report_number) not in os.listdir('mhealth_reports'):
                # Just download the report if it hasn't been downloaded yet
                print("Downloading report %i" %report_number)
                request = requests.get(url.format(index=report_number))
                if request.status_code < 400:
                    with open("mhealth_reports/{number}.pdf".format(number=report_number), "wb") as file:
                        file.write(request.content)
                else:
                    # No more reports available
                    break
            report_number += 1
