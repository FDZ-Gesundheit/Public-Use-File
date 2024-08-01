import unittest
import pandas as pd
from helpers import connect_to_database
from functions import force_k, shuffle_column


class TestDatabase(unittest.TestCase):

    def test_database_connection(self):
        # test sqlite connection for local testing purposes
        self.assertIsNot(connect_to_database("sqlite", 'fdz', 'fdz'), False)


class TestDataProcessing(unittest.TestCase):

    def test_k_anonymity(self):
        k = 3
        print(f"Test k_anonymity with k={k}")
        int_values = pd.Series([1, 1, 1, 1, 3, 3, 3, 5, 5, 5, 5, 5])
        float_values = pd.Series([0.5, 0.7, 1.3, 1.3, 0.8, 1.4, 0.5, 0.7, 1.2, 1.1, 1.2, 1.2, 1.2, 1.0])
        cat_values = pd.Series(['L', 'R', 'L', 'B', 'B', 'B', 'L', 'R', 'R', 'R', 'F', 'I'])
        cat_values_nan = pd.Series(['L', 'R', 'L', None, 'B', 'B', None, 'B', 'R', None, 'R', 'R'])
        year_values = pd.Series([2010, 2010, 2011, 2011, 2012, 2012, 2012, 2013, 2014])
        for values, data_type in zip([int_values, float_values, cat_values, cat_values_nan, year_values],
                                     ['integer', 'float', 'category', 'category', 'year']):
            print("before\t\t", list(values))
            k_values = force_k(values, data_type, k)
            print("afterwards\t", list(k_values), "\n")
            df = pd.DataFrame({'values': list(k_values)})
            number_counts = df['values'].value_counts()
            self.assertTrue(number_counts[number_counts < k].empty)

    def test_column_shuffling(self):
        print(f"Test random column shuffling, i.e. assert that the values have a new order.")
        input_values = pd.Series([1, 3, 1, 5, 3, 7, 3, 5, 6, 5])
        print("Input values:\t", list(input_values))
        output_values = shuffle_column(input_values)
        print("Output values:\t", list(output_values), "\n")
        self.assertIs(input_values.equals(output_values), False)
        self.assertEqual(set(list(input_values)), set(list(output_values)))

    def test_randomness(self):
        print("Test randomness of column shuffling, i.e we shuffle the same column twice and check that the output is "
              "different")
        input_values = pd.Series([1, 3, 1, 5, 3, 7, 3, 5, 6, 5])
        print("Input values:\t\t", list(input_values))
        output_values = shuffle_column(input_values)
        print("Output values 1:\t", list(output_values))
        output_values_1 = shuffle_column(input_values)
        print("Output values 2:\t", list(output_values_1))
        self.assertIs(output_values.equals(output_values_1), False)


if __name__ == '__main__':

    unittest.main(argv=['', '-v'])
