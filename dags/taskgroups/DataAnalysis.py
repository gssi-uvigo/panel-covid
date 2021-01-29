"""
    Analyze the data stored in the database, after the download and extraction processes have finished.
"""
import pandas as pd
import numpy as np
from datetime import datetime as dt

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from AuxiliaryFunctions import MongoDatabase


class DailyCOVIDData:
    """
        Daily data of the COVID pandemic in Spain, with the number of new cases, hospitalizations, and deaths by
        Autonomous Region and age range.
    """

    @staticmethod
    def calculate_increase_percentage(data):
        """Return the percentage increase or decrease in the new cases, deaths, or hospitalizations"""
        if data[0] == 0:
            return 0
        return 100 * ((data[-1] - data[0]) / data[0])

    def __init__(self):
        """Load the data from the database and store it into a Pandas DataFrame"""
        # Connection to the extracted data database for reading, and to the analyzed data for writing
        self.db_read = MongoDatabase(MongoDatabase.extracted_db_name)
        self.db_write = MongoDatabase(MongoDatabase.analyzed_db_name)

        # Load the data from the DB
        self.df = self.db_read.read_data('daily_data')
        self.population_df = self.db_read.read_data('population_ar')

        # Aggregate the data
        self.__merge__population__()

    def __merge__population__(self):
        """Merge the COVID daily data dataset with the population dataset"""

        # Change the population age ranges to the COVID daily data ones
        age_range_translations = {'0-4': '0-9', '5-9': '0-9', '10-14': '10-19', '15-19': '10-19', '20-24': '20-29',
                                  '25-29': '20-29', '30-34': '30-39', '35-39': '30-39', '40-44': '40-49',
                                  '45-49': '40-49', '50-54': '50-59', '55-59': '50-59', '60-64': '60-69',
                                  '65-69': '60-69', '70-74': '70-79', '75-79': '70-79', '80-84': '80+', '85-89': '80+',
                                  '≥90': '80+', 'Total': 'total'}
        self.population_df['age_range'] = self.population_df['age_range'].replace(age_range_translations)
        self.population_df = self.population_df.groupby(['age_range', 'autonomous_region']).sum().reset_index()

        # Replace the M, F, total columns by a single 'gender' column
        self.population_df = self.population_df.melt(id_vars=['autonomous_region', 'age_range'],
                                                     value_vars=['M', 'F', 'total'],
                                                     var_name='gender')

        # Merge the COVID dataset with the population data
        covid_population_df = pd.merge(self.df, self.population_df, on=['autonomous_region', 'age_range', 'gender']) \
            .rename(columns={'value': 'population'})
        covid_population_df['date'] = pd.to_datetime(covid_population_df['date'])
        self.df = covid_population_df.set_index('date')

    def process_and_store_cases(self):
        """Create a DataFrame with all the data related to the cases"""
        # Get only the cases from the dataset
        cases_df = self.df.copy()[
            ['gender', 'age_range', 'autonomous_region', 'new_cases', 'total_cases', 'population']]

        # Calculate the cases per population
        cases_df['new_cases_per_population'] = 100000 * cases_df['new_cases'] / cases_df['population']
        cases_df['total_cases_per_population'] = 100000 * cases_df['total_cases'] / cases_df['population']

        # CI last 14 days
        cases_ci = cases_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_cases_per_population'].rolling(
            '14D', min_periods=1).sum()
        cases_df = pd.merge(cases_df, cases_ci, on=['autonomous_region', 'date', 'gender', 'age_range']).rename(
            columns={'new_cases_per_population_x': 'new_cases_per_population',
                     'new_cases_per_population_y': 'ci_last_14_days'})
        cases_df['inverted_ci'] = cases_df['ci_last_14_days'].apply(lambda x: 100000 / x if x > 10 else 10000)

        # Daily, weekly and monthly increase
        increase_cases_df_1d = cases_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_cases'].rolling(
            '7D').mean().rolling(2).apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_cases_df_7d = cases_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_cases'].rolling(
            '14D').mean().rolling(8).apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_cases_df_30d = cases_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_cases'].rolling(
            '60D').mean().rolling(31).apply(DailyCOVIDData.calculate_increase_percentage, raw=True)

        increase_cases_percentages = pd.DataFrame(
            {'daily_increase': increase_cases_df_1d, 'weekly_increase': increase_cases_df_7d,
             'monthly_increase': increase_cases_df_30d})
        cases_df = pd.merge(cases_df, increase_cases_percentages,
                            on=['autonomous_region', 'date', 'age_range', 'gender'])

        # New cases moving average
        new_cases_ma_1w = cases_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            'new_cases_per_population'].rolling('8D').mean()
        new_cases_ma_2w = cases_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            'new_cases_per_population'].rolling('15D').mean()
        new_cases_ma = pd.DataFrame({'new_cases_ma_1w': new_cases_ma_1w, 'new_cases_ma_2w': new_cases_ma_2w})
        cases_df = pd.merge(cases_df, new_cases_ma, on=['autonomous_region', 'date', 'age_range', 'gender'])

        cases_df = cases_df.drop(columns=['population'])

        # Store the data
        self.db_write.store_data('cases', cases_df.reset_index().to_dict('records'))

    def process_and_store_deaths(self):
        """Create a DataFrame with all the data related to the deaths"""
        # Get only the deaths from the dataset
        deaths_df = self.df.copy()[['gender', 'age_range', 'autonomous_region', 'new_deaths', 'total_deaths',
                                    'new_cases', 'total_cases', 'population']]

        # Calculate the deaths per population
        deaths_df['new_deaths_per_population'] = 100000 * deaths_df['new_deaths'] / deaths_df['population']
        deaths_df['total_deaths_per_population'] = 100000 * deaths_df['total_deaths'] / deaths_df['population']

        # Daily, weekly and monthly increase
        increase_deaths_df_1d = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_deaths'].rolling(
            '2D').apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_deaths_df_7d = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_deaths'].rolling(
            '8D').apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_deaths_df_14d = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_deaths'].rolling(
            '15D').apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_deaths_df_30d = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])['new_deaths'].rolling(
            '31D').apply(DailyCOVIDData.calculate_increase_percentage, raw=True)

        increase_deaths_percentages = pd.DataFrame(
            {'daily_increase': increase_deaths_df_1d, 'weekly_increase': increase_deaths_df_7d,
             'two_weeks_increase': increase_deaths_df_14d, 'monthly_increase': increase_deaths_df_30d})
        deaths_df = pd.merge(deaths_df, increase_deaths_percentages,
                             on=['autonomous_region', 'date', 'age_range', 'gender'])

        # New deaths moving average
        new_deaths_ma_1w = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            'new_deaths_per_population'].rolling('8D').mean()
        new_deaths_ma_2w = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            'new_deaths_per_population'].rolling('15D').mean()
        new_deaths_ma = pd.DataFrame({'new_deaths_ma_1w': new_deaths_ma_1w, 'new_deaths_ma_2w': new_deaths_ma_2w})
        deaths_df = pd.merge(deaths_df, new_deaths_ma, on=['autonomous_region', 'date', 'age_range', 'gender'])

        # Mortality percentage
        deaths_df['new_cases_per_population'] = 100000 * deaths_df['new_cases'] / deaths_df['population']
        new_cases_ma_2w = deaths_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            'new_cases_per_population'].rolling('15D').mean()
        new_cases_ma_2w_df = pd.DataFrame({'new_cases_ma_2w': new_cases_ma_2w})
        deaths_df = pd.merge(deaths_df, new_cases_ma_2w_df, on=['autonomous_region', 'date', 'age_range', 'gender'])
        deaths_df['mortality_2w'] = 100 * (deaths_df['new_deaths_ma_2w'] / deaths_df['new_cases_ma_2w']). \
            replace(np.nan, 0)
        deaths_df['mortality_total'] = 100 * (deaths_df['total_deaths'] / deaths_df['total_cases']).replace(np.nan, 0)

        deaths_df = deaths_df.drop(
            columns=['new_cases_ma_2w', 'new_cases_per_population', 'new_cases', 'total_cases', 'population'])

        # Store the data
        self.db_write.store_data('deaths', deaths_df.reset_index().to_dict('records'))

    def process_and_store_hospitalizations(self):
        """Create a DataFrame with all the data related to the hospitalizations"""
        # Get only the hospitalizations from the dataset
        hospitalizations_df = self.df.copy()[
            ['gender', 'age_range', 'autonomous_region', 'new_hospitalizations', 'total_hospitalizations',
             'new_ic_hospitalizations', 'total_ic_hospitalizations', 'new_cases', 'total_cases', 'population']]

        # Calculate the hospitalizations per population
        hospitalizations_df['new_hospitalizations_per_population'] = 100000 * hospitalizations_df[
            'new_hospitalizations'] / hospitalizations_df['population']
        hospitalizations_df['total_hospitalizations_per_population'] = 100000 * hospitalizations_df[
            'total_hospitalizations'] / hospitalizations_df['population']
        hospitalizations_df['new_ic_hospitalizations_per_population'] = 100000 * hospitalizations_df[
            'new_ic_hospitalizations'] / hospitalizations_df['population']
        hospitalizations_df['total_ic_hospitalizations_per_population'] = 100000 * hospitalizations_df[
            'total_ic_hospitalizations'] / hospitalizations_df['population']

        # Daily, weekly and monthly increase
        increase_hospitalizations_df_1d = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            ['new_hospitalizations', 'new_ic_hospitalizations']].rolling('2D'). \
            apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_hospitalizations_df_7d = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            ['new_hospitalizations', 'new_ic_hospitalizations']].rolling('8D'). \
            apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_hospitalizations_df_14d = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            ['new_hospitalizations', 'new_ic_hospitalizations']].rolling('15D'). \
            apply(DailyCOVIDData.calculate_increase_percentage, raw=True)
        increase_hospitalizations_df_30d = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            ['new_hospitalizations', 'new_ic_hospitalizations']].rolling('31D'). \
            apply(DailyCOVIDData.calculate_increase_percentage, raw=True)

        increase_hospitalizations_percentages = pd.DataFrame(
            {'hospitalizations_daily_increase': increase_hospitalizations_df_1d['new_hospitalizations'],
             'hospitalizations_weekly_increase': increase_hospitalizations_df_7d['new_hospitalizations'],
             'hospitalizations_two_weeks_increase': increase_hospitalizations_df_14d['new_hospitalizations'],
             'hospitalizations_monthly_increase': increase_hospitalizations_df_30d['new_hospitalizations'],
             'ic_daily_increase': increase_hospitalizations_df_1d['new_ic_hospitalizations'],
             'ic_weekly_increase': increase_hospitalizations_df_7d['new_ic_hospitalizations'],
             'ic_two_weeks_increase': increase_hospitalizations_df_14d['new_ic_hospitalizations'],
             'ic_monthly_increase': increase_hospitalizations_df_30d['new_ic_hospitalizations']})
        hospitalizations_df = pd.merge(hospitalizations_df, increase_hospitalizations_percentages,
                                       on=['autonomous_region', 'date', 'age_range', 'gender'])

        # New hospitalizations moving average
        new_hospitalizations_ma_1w = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            ['new_hospitalizations_per_population', 'new_ic_hospitalizations_per_population']].rolling('8D').mean()
        new_hospitalizations_ma_2w = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            ['new_hospitalizations_per_population', 'new_ic_hospitalizations_per_population']].rolling('15D').mean()
        new_hospitalizations_ma = pd.DataFrame(
            {'new_hospitalizations_ma_1w': new_hospitalizations_ma_1w['new_hospitalizations_per_population'],
             'new_hospitalizations_ma_2w': new_hospitalizations_ma_2w['new_hospitalizations_per_population'],
             'new_ic_ma_1w': new_hospitalizations_ma_1w['new_ic_hospitalizations_per_population'],
             'new_ic_ma_2w': new_hospitalizations_ma_2w['new_ic_hospitalizations_per_population']})
        hospitalizations_df = pd.merge(hospitalizations_df, new_hospitalizations_ma,
                                       on=['autonomous_region', 'date', 'age_range', 'gender'])

        # Hospitalization percentage
        hospitalizations_df['new_cases_per_population'] = \
            100000 * hospitalizations_df['new_cases'] / hospitalizations_df['population']
        new_cases_ma_2w = hospitalizations_df.groupby(['autonomous_region', 'gender', 'age_range'])[
            'new_cases_per_population'].rolling('15D').mean()
        new_cases_ma_2w_df = pd.DataFrame({'new_cases_ma_2w': new_cases_ma_2w})
        hospitalizations_df = pd.merge(hospitalizations_df, new_cases_ma_2w_df,
                                       on=['autonomous_region', 'date', 'age_range', 'gender'])
        hospitalizations_df['hospitalization_ratio_2w'] = 100 * (
                hospitalizations_df['new_hospitalizations_ma_2w'] / hospitalizations_df['new_cases_ma_2w']).replace(
            np.nan, 0)
        hospitalizations_df['hospitalization_ratio_total'] = 100 * (
                hospitalizations_df['total_hospitalizations'] / hospitalizations_df['total_cases']).replace(np.nan,
                                                                                                            0)
        hospitalizations_df['hospitalization_ic_ratio_2w'] = 100 * (
                hospitalizations_df['new_ic_ma_2w'] / hospitalizations_df['new_cases_ma_2w']).replace(np.nan, 0)
        hospitalizations_df['hospitalization_ic_ratio_total'] = 100 * (
                hospitalizations_df['total_ic_hospitalizations'] / hospitalizations_df['total_cases']).replace(
            np.nan, 0)

        hospitalizations_df = hospitalizations_df.drop(
            columns=['new_cases_ma_2w', 'new_cases_per_population', 'new_cases', 'total_cases', 'population'])

        # Store the data
        self.db_write.store_data('hospitalizations', hospitalizations_df.reset_index().to_dict('records'))


class DeathCauses:
    """Death causes in Spain"""

    age_range_translations = {'0-1': '0-9', '1-4': '0-9', '5-9': '0-9', '10-14': '10-19', '15-19': '10-19',
                              '20-24': '20-29',
                              '25-29': '20-29', '30-34': '30-39', '35-39': '30-39', '40-44': '40-49',
                              '45-49': '40-49', '50-54': '50-59', '55-59': '50-59', '60-64': '60-69',
                              '65-69': '60-69', '70-74': '70-79', '75-79': '70-79', '80-84': '80+', '85-89': '80+',
                              '90-94': '80+', '95+': '80+', 'Total': 'total'}

    def __init__(self):
        """Load the datasets"""
        # Connection to the extracted data database for reading, and to the analyzed data for writing
        self.db_read = MongoDatabase(MongoDatabase.extracted_db_name)
        self.db_write = MongoDatabase(MongoDatabase.analyzed_db_name)

        # Load the death causes and COVID deaths datasets
        self.death_causes_df = self.db_read.read_data('death_causes')
        self.deaths_df = self.db_write.read_data('deaths', {'autonomous_region': 'España',
                                                            'date': dt(2021, 1, 20)},
                                                 ['age_range', 'total_deaths', 'gender'])

    def process_and_store_data(self):
        """Analyze the data, calculate some new variables, and store the results to the database"""
        self.__calculate_top_10_death_causes__()
        self.__store_data__()

    def __calculate_top_10_death_causes__(self):
        """Calculate the top 10 death causes in Spain and the percentage of total deaths whose cause was COVID"""
        # Use the same age ranges in the three dataframes
        self.death_causes_df['age_range'] = self.death_causes_df['age_range']. \
            replace(DeathCauses.age_range_translations)
        self.death_causes_df = self.death_causes_df.groupby(['age_range', 'death_cause', 'gender']).sum().reset_index()

        # Get "all causes" death cause and then remove it
        all_causes_sum_df = self.death_causes_df[self.death_causes_df['death_cause'] == 'Todas las causas'].copy()
        death_causes_df = self.death_causes_df[self.death_causes_df['death_cause'] != 'Todas las causas']

        # Group the 2018 death causes with the COVID-19 deaths
        self.deaths_df['death_cause'] = 'COVID-19'
        death_causes_total = pd.concat([self.deaths_df, death_causes_df])

        # Get the top 10 death causes for each age range and gender
        death_causes_top_10 = death_causes_total.sort_values(['age_range', 'total_deaths', 'gender'],
                                                             ascending=False).groupby(['age_range', 'gender']).head(10)
        death_causes_top_10['total_deaths'] = death_causes_top_10['total_deaths'].round().astype("int")
        self.death_causes_top_10 = death_causes_top_10

        # Calculate the percentage of deaths produced by COVID
        covid_vs_all_deaths = pd.merge(self.deaths_df, all_causes_sum_df, on=['age_range', 'gender']).rename(
            columns={'total_deaths_x': 'covid_deaths', 'total_deaths_y': 'other_deaths'}).drop(
            columns=['death_cause_y', 'death_cause_x'])
        covid_vs_all_deaths['covid_percentage'] = 100 * covid_vs_all_deaths['covid_deaths'] / (
                covid_vs_all_deaths['covid_deaths'] + covid_vs_all_deaths['other_deaths'])
        self.covid_vs_all_deaths = covid_vs_all_deaths

    def __store_data__(self):
        """Store the top death causes and the COVID deaths percentage in the database"""
        mongo_data_top_death_causes = self.death_causes_top_10.to_dict('records')
        collection = 'top_death_causes'
        self.db_write.store_data(collection, mongo_data_top_death_causes)

        mongo_data_covid_vs_all_deaths = self.covid_vs_all_deaths.to_dict('records')
        collection = 'covid_vs_all_deaths'
        self.db_write.store_data(collection, mongo_data_covid_vs_all_deaths)


class DiagnosticTests:
    """Dataset with the number of diagnostic tests made each day on each Autonomous Region"""

    def __init__(self):
        """Load the datasets"""
        # Connection to the extracted data database for reading, and to the analyzed data for writing
        self.db_read = MongoDatabase(MongoDatabase.extracted_db_name)
        self.db_write = MongoDatabase(MongoDatabase.analyzed_db_name)

        # Load the diagnostic tests, Spanish population and COVID cases datasets
        self.diagnostic_tests_df = self.db_read.read_data('diagnostic_tests')
        self.population_df = self.db_read.read_data('population_ar', {'age_range': 'total'},
                                                    ['autonomous_region', 'total'])
        self.new_cases_df = self.db_read.read_data('daily_data', {'age_range': 'total', 'gender': 'total'},
                                                   ['date', 'autonomous_region', 'new_cases'])

    def __process_dataset__(self):
        """
            Get the data for the whole country, the total number of tests, the average positivity, and the number of
            total tests per 100k inhabitants.
        """
        # Number of tests and average positivity in the whole country
        diagnostics_grouped = self.diagnostic_tests_df.groupby('date')
        diagnostics_total_tests = diagnostics_grouped['total_diagnostic_tests'].sum()
        diagnostics_avg_positivity = diagnostics_grouped['positivity'].mean()
        diagnostics_df_total = pd.merge(diagnostics_total_tests, diagnostics_avg_positivity,
                                        on='date').reset_index()

        diagnostics_df_total['autonomous_region'] = 'España'
        self.diagnostic_tests_df = pd.concat([self.diagnostic_tests_df, diagnostics_df_total])
        self.diagnostic_tests_df = self.diagnostic_tests_df.sort_values(by=['date', 'autonomous_region'])

        # Number of total tests
        diagnostic_tests_df_total = self.diagnostic_tests_df[['date', 'autonomous_region', 'total_diagnostic_tests']] \
            .groupby(['date', 'autonomous_region']).sum().groupby('autonomous_region').cumsum().reset_index()
        self.diagnostic_tests_df = pd.merge(self.diagnostic_tests_df, diagnostic_tests_df_total,
                                            on=['date', 'autonomous_region']).rename(
            columns={'total_diagnostic_tests_x': 'new_diagnostic_tests',
                     'total_diagnostic_tests_y': 'total_diagnostic_tests'})

        # Average positivity for each Autonomous Region
        diagnostic_tests_df_avg_positivity = self.diagnostic_tests_df[
            ['date', 'autonomous_region', 'positivity']].groupby(['date', 'autonomous_region']).sum().groupby(
            'autonomous_region')
        avg_positivity_df = diagnostic_tests_df_avg_positivity.cumsum().rename(columns={'positivity': 'sum'})
        avg_positivity_df['count'] = diagnostic_tests_df_avg_positivity.cumcount()
        avg_positivity_df['average_positivity'] = avg_positivity_df['sum'] / avg_positivity_df['count']
        avg_positivity_df = avg_positivity_df.drop(columns=['sum', 'count'])
        self.diagnostic_tests_df = pd.merge(self.diagnostic_tests_df, avg_positivity_df,
                                            on=['date', 'autonomous_region'])

        # Total tests / 100 000 inhabitants
        diagnostics_population_df = pd.merge(self.diagnostic_tests_df, self.population_df, on='autonomous_region') \
            .rename(columns={'total': 'population'})
        diagnostics_population_df['total_tests_per_population'] = 100000 * diagnostics_population_df[
            'total_diagnostic_tests'] / diagnostics_population_df['population']
        self.diagnostic_tests_df = diagnostics_population_df.drop(columns='population')

    def __append_daily_cases__(self):
        """
            Append the daily cases to the diagnostic tests dataset, to enable some multi-dimensional visualizations.
        """
        self.diagnostic_tests_df = pd.merge(self.diagnostic_tests_df, self.new_cases_df,
                                            on=['date', 'autonomous_region'])

    def __store_data__(self):
        """Store the processed dataset in the database"""
        mongo_data = self.diagnostic_tests_df.to_dict('records')
        collection = 'diagnostic_tests'
        self.db_write.store_data(collection, mongo_data)

    def process_and_store(self):
        """Analyze the data, calculate some new variables, and store the results to the database"""
        self.__process_dataset__()
        self.__append_daily_cases__()
        self.__store_data__()


class OutbreaksDescription:
    """Outbreaks description in Spain"""

    def __init__(self):
        """Load the dataset"""
        # Connection to the extracted data database for reading, and to the analyzed data for writing
        self.db_read = MongoDatabase(MongoDatabase.extracted_db_name)
        self.db_write = MongoDatabase(MongoDatabase.analyzed_db_name)

        # Load the outbreaks description
        self.outbreaks_description_df = self.db_read.read_data('outbreaks_description')

    def move_data(self):
        """Just move the data from the extracted to the analyzed database"""
        self.__store_data__()

    def __store_data__(self):
        """Store the outbreaks description in the database"""
        mongo_data = self.outbreaks_description_df.to_dict('records')
        collection = 'outbreaks_description'
        self.db_write.store_data(collection, mongo_data)


class HospitalsPressure:
    """Hospitals pressure in Spain"""

    def __init__(self):
        """Load the dataset"""
        # Connection to the extracted data database for reading, and to the analyzed data for writing
        self.db_read = MongoDatabase(MongoDatabase.extracted_db_name)
        self.db_write = MongoDatabase(MongoDatabase.analyzed_db_name)

        # Load the hospitals pressure data
        self.hospitals_pressure = self.db_read.read_data('hospitals_pressure',
                                                         projection=['hospitalized_patients', 'beds_percentage',
                                                                     'ic_patients', 'ic_beds_percentage'])

    def move_data(self):
        """Just move the data from the extracted to the analyzed database"""
        self.__store_data__()

    def __store_data__(self):
        """Store the outbreaks description in the database"""
        mongo_data = self.hospitals_pressure.to_dict('records')
        collection = 'hospitals_pressure'
        self.db_write.store_data(collection, mongo_data)


class TransmissionIndicators:
    """
        Transmission indicators in Spain: cases with unknown contact,
        identified contacts per case and asymptomatic cases percentage.
    """

    def __init__(self):
        """Load the dataset"""
        # Connection to the extracted data database for reading, and to the analyzed data for writing
        self.db_read = MongoDatabase(MongoDatabase.extracted_db_name)
        self.db_write = MongoDatabase(MongoDatabase.analyzed_db_name)

        # Load the transmission indicators data
        self.transmission_indicators = self.db_read.read_data('transmission_indicators')

    def __transform_data__(self):
        """Get only the desired data and transform it to a single-level hierarchy"""
        ti_df = self.transmission_indicators
        ti_df['cases_unknown_contact'] = ti_df['transmission_indicators'].apply(
            lambda x: x['cases_unknown_contact']['percentage'])
        ti_df['identified_contacts_per_case'] = ti_df['transmission_indicators'].apply(
            lambda x: x['identified_contacts_per_case']['median'])
        ti_df['asymptomatic_percentage'] = ti_df['transmission_indicators'].apply(
            lambda x: x['asymptomatic_percentage'])
        self.transmission_indicators = ti_df.drop(columns='transmission_indicators')

    def move_data(self):
        """Just move the data from the extracted to the analyzed database"""
        self.__transform_data__()
        self.__store_data__()

    def __store_data__(self):
        """Store the outbreaks description in the database"""
        mongo_data = self.transmission_indicators.to_dict('records')
        collection = 'transmission_indicators'
        self.db_write.store_data(collection, mongo_data)


class DataAnalysisTaskGroup(TaskGroup):
    """TaskGroup that analyzes all the downloaded and extracted data."""

    def __init__(self, dag):
        # Instantiate the TaskGroup
        super(DataAnalysisTaskGroup, self) \
            .__init__("data_analysis",
                      tooltip="Analyze all the downloaded and extracted data",
                      dag=dag)

        # Instantiate the operators
        PythonOperator(task_id='analyze_cases_data',
                       python_callable=DataAnalysisTaskGroup.analyze_daily_cases,
                       task_group=self,
                       dag=dag)

        analyze_deaths_data_op = PythonOperator(task_id='analyze_deaths_data',
                                                python_callable=DataAnalysisTaskGroup.analyze_daily_deaths,
                                                task_group=self,
                                                dag=dag)

        PythonOperator(task_id='analyze_hospitalizations_data',
                       python_callable=DataAnalysisTaskGroup.analyze_daily_hospitalizations,
                       task_group=self,
                       dag=dag)

        analyze_death_causes_op = PythonOperator(task_id='analyze_death_causes',
                                                 python_callable=DataAnalysisTaskGroup.analyze_death_causes,
                                                 task_group=self,
                                                 dag=dag)

        analyze_deaths_data_op >> analyze_death_causes_op

        PythonOperator(task_id='move_outbreaks_description',
                       python_callable=DataAnalysisTaskGroup.move_outbreaks_description,
                       task_group=self,
                       dag=dag)

        PythonOperator(task_id='move_hospitals_pressure',
                       python_callable=DataAnalysisTaskGroup.move_hospitals_pressure,
                       task_group=self,
                       dag=dag)

        PythonOperator(task_id='analyze_diagnostic_tests_data',
                       python_callable=DataAnalysisTaskGroup.analyze_diagnostic_tests,
                       task_group=self,
                       dag=dag)

        PythonOperator(task_id='move_transmission_indicators',
                       python_callable=DataAnalysisTaskGroup.move_transmission_indicators,
                       task_group=self,
                       dag=dag)

    @staticmethod
    def analyze_daily_cases():
        """Analyze the cases data in the daily COVID dataset"""
        data = DailyCOVIDData()
        data.process_and_store_cases()

    @staticmethod
    def analyze_daily_deaths():
        """Analyze the deaths data in the daily COVID dataset"""
        data = DailyCOVIDData()
        data.process_and_store_deaths()

    @staticmethod
    def analyze_daily_hospitalizations():
        """Analyze the hospitalizations data in the daily COVID dataset"""
        data = DailyCOVIDData()
        data.process_and_store_hospitalizations()

    @staticmethod
    def analyze_death_causes():
        """Extract the top 10 death causes and compare them with COVID-19"""
        data = DeathCauses()
        data.process_and_store_data()

    @staticmethod
    def move_outbreaks_description():
        """Move the outbreaks description from the extracted to the analyzed database"""
        data = OutbreaksDescription()
        data.move_data()

    @staticmethod
    def move_hospitals_pressure():
        """Move the hospitals pressure data from the extracted to the analyzed database"""
        data = HospitalsPressure()
        data.move_data()

    @staticmethod
    def analyze_diagnostic_tests():
        """Analyze the diagnostic tests data"""
        data = DiagnosticTests()
        data.process_and_store()

    @staticmethod
    def move_transmission_indicators():
        """Move the transmission indicators data from the extracted to the analyzed database"""
        data = TransmissionIndicators()
        data.move_data()
