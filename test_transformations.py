import unittest
import pandas as pd
from transformations import flag_retention_observations_weekly, calculate_retention


class TestRetention(unittest.TestCase):
    def setUp(self):
        self.test_df = pd.read_csv('test_data.csv', sep=';')
        self.test_df['periode'] = pd.to_datetime(self.test_df['periode'], format='%d.%m.%Y')

        df = pd.read_csv('test_data_multidim.csv', sep=';')
        df['periode'] = pd.to_datetime(df['periode'], format='%d.%m.%Y')
        self.test_df_multi = df.copy()

    def test_single_groupby_true(self):
        df = flag_retention_observations_weekly(self.test_df, 'navn', [], 'periode')
        lookup_val = df.loc[(df['navn'] == 'iben') & (df['week'] == 52), 'retention'].values[0]
        self.assertTrue(lookup_val)

    def test_single_groupby_false(self):
        df = flag_retention_observations_weekly(self.test_df, 'navn', [], 'periode')
        lookup_val = df.loc[(df['navn'] == 'iben') & (df['week'] == 1), 'retention'].values[0]
        self.assertFalse(lookup_val)

    def test_multi_dim_no(self):
        df = flag_retention_observations_weekly(self.test_df_multi, 'navn', ['tabell'], 'periode')
        lookup_val = df.loc[(df['navn'] == 'iben') & (df['week'] == 1) & (df['tabell'] == 'arbeidspenger'), 'retention'].values[0]
        self.assertTrue(lookup_val)

    def test_not_all_true(self):
        df = flag_retention_observations_weekly(self.test_df, 'navn', [], 'periode')
        lookup_val = len(df[df['retention'].notnull()])
        self.assertTrue(lookup_val > 0)


class TestRetetntion(unittest.TestCase):
    def setUp(self):
        self.test_df = pd.read_csv('test_data.csv', sep=';')
        self.test_df['periode'] = pd.to_datetime(self.test_df['periode'], format='%d.%m.%Y')

        df = pd.read_csv('test_data_multidim.csv', sep=';')
        df['periode'] = pd.to_datetime(df['periode'], format='%d.%m.%Y')
        self.test_df_multi = df.copy()

    def test_retention_groupby_no_groupby(self):
        df = calculate_retention(self.test_df_multi, 'navn', [], 'periode')
        lookup_val = df.loc[df['week'] == 1, 'retention_share'].values[0]
        self.assertEqual(lookup_val, 2/3)

    def test_retention_groupby(self):
        df = calculate_retention(self.test_df_multi, 'navn', ['tabell'], 'periode')
        lookup_val = df.loc[(df['week'] == 1) & (df['tabell'] == 'sykepenger'), 'retention_share'].values[0]
        self.assertEqual(lookup_val, 1/3)


if __name__ == '__main__':
    unittest.main()