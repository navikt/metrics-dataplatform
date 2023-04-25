import unittest

import pandas as pd
from transformations import flag_observation_in_window, calculate_retention_weekly, extract_tables_from_query


class TestFlagWindow(unittest.TestCase):
    def setUp(self):
        df = pd.read_csv('test_data_multidim.csv', sep=';')
        df['periode'] = pd.to_datetime(df['periode'], format='%d.%m.%Y')
        self.test_df_multi = df.copy()

        self.df_forward = flag_observation_in_window(self.test_df_multi.copy(), 'navn', ['tabell'], 'periode')
        self.df_backward = flag_observation_in_window(self.test_df_multi.copy(), 'navn', ['tabell'], 'periode', forward_window=False)

    def test_forward_observation(self):
        df = self.df_forward
        lookup_val = df.loc[(df['navn'] == 'hedvig') & (df['date'] == '2022-01-19') & (
                    df['tabell'] == 'sykepenger'), 'observations_next_window'].values[0]
        self.assertTrue(lookup_val)

    def test_observation_right_edge(self):
        df = self.df_forward
        lookup_val = df.loc[(df['navn'] == 'hedvig') & (df['date'] == '2022-01-24') & (
                df['tabell'] == 'sykepenger'), 'observations_next_window'].values[0]
        self.assertEqual(lookup_val, 0)

    def test_observation_backward(self):
        df = self.df_backward
        lookup_val = df.loc[(df['navn'] == 'iben') & (df['date'] == '2022-01-06') & (
                df['tabell'] == 'sykepenger'), 'observations_this_window'].values[0]
        self.assertTrue(lookup_val)

    def test_observation_backward_right_edge(self):
        df = self.df_backward
        lookup_val = df.loc[(df['navn'] == 'hedvig') & (df['date'] == '2022-01-24') & (
                df['tabell'] == 'sykepenger'), 'observations_this_window'].values[0]
        self.assertTrue(lookup_val)

    def test_observation_inner_edge_this_window(self):
        df = self.df_backward
        lookup_val = df.loc[(df['navn'] == 'hedvig') & (df['date'] == '2022-01-14') & (
                df['tabell'] == 'arbeidspenger'), 'observations_this_window'].values[0]
        self.assertTrue(lookup_val)

    def test_observation_inner_edge_forward_window(self):
        df = self.df_forward
        lookup_val = df.loc[(df['navn'] == 'hedvig') & (df['date'] == '2022-01-14') & (
                df['tabell'] == 'arbeidspenger'), 'observations_next_window'].values[0]
        self.assertTrue(lookup_val)

    def test_observations_future_periods(self):
        df = self.df_forward
        lookup_val = df['date'].unique()


class TestRetention(unittest.TestCase):
    def setUp(self):
        df = pd.read_csv('test_data_multidim.csv', sep=';')
        df['periode'] = pd.to_datetime(df['periode'], format='%d.%m.%Y')
        self.df = calculate_retention_weekly(df, 'navn', ['tabell'], 'periode')

    def test_retention_zero(self):
        df = self.df
        lookup_val = df.loc[(df['date'] == '2022-01-13') & (df['tabell'] == 'sykepenger'), 'retention'].values[0]
        self.assertEqual(lookup_val, 0)
    def test_retention_full_retention(self):
        df = self.df
        lookup_val = df.loc[(df['date'] == '2022-01-02') & (df['tabell'] == 'sykepenger'), 'retention'].values[0]
        self.assertEqual(lookup_val, 1)

    def test_retention_partly_on_edge(self):
        df = self.df
        lookup_val = df.loc[(df['date'] == '2022-01-07') & (df['tabell'] == 'sykepenger'), 'retention'].values[0]
        self.assertEqual(lookup_val, 1/3)

    def test_retention_partly_not_edge(self):
        df = self.df
        lookup_val = df.loc[(df['date'] == '2022-01-08') & (df['tabell'] == 'sykepenger'), 'retention'].values[0]
        self.assertEqual(lookup_val, 1/3)



if __name__ == '__main__':
    unittest.main()
