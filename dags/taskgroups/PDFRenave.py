"""
    Download the RENAVE reports, extract the data from the PDFs, and store it in the database.
"""

import os
import re
import requests

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup


class PDFRenaveTaskGroup(TaskGroup):
    """TaskGroup that downloads the RENAVE reports, extracts the data, and stores it into the database"""

    def __init__(self, dag):
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
                if not os.path.exists('renave_reports/{number}.pdf'.format(number=number)):
                    # Download the file only if it had not been previously downloaded
                    print("Downloading report number %i: %s" % (number, report_url.replace('%20', ' ')))
                    request = requests.get(base_url + report_url)
                    if request.status_code < 400:
                        with open("renave_reports/{number}.pdf".format(number=number), "wb") as file:
                            file.write(request.content)