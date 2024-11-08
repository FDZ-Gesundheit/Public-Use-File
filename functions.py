import string
import secrets
import sys

import pandas as pd
import numpy as np
import random

from helpers import get_constant_variables, get_pseudo_variables

POSSIBLE_CHARACTERS = string.ascii_uppercase + string.ascii_lowercase + string.digits
UNCHANGED_VARIABLES = get_pseudo_variables() + get_constant_variables()


def generate_secure_bytes(size: int, n_bytes=32):
    """Generate secure byte strings using secrets.token_hex.
    This uses the most secure source of randomness that your operating system provides.

    Parameters:
        size: Length of array to be returned.
        n_bytes: Number of bytes to generate a random byte string.
    Returns:
        numpy.ndarray: Array of secure random bytes.
    """
    return np.array([secrets.token_hex(n_bytes) for _ in range(size)])


def shuffle_column(variable: pd.Series):
    """Shuffle values based on random bytes.

    Parameters:
        variable (array-like): Input vector of any length.

    Returns:
        pandas.Series: The same vector, randomly ordered.
    """
    # Generate random integers for sorting
    random_integers = generate_secure_bytes(len(variable))

    # Sort variable using the random bytes
    # Returns the indices that would sort an array.
    sorted_indices = np.argsort(random_integers)
    shuffled_variable = np.array(variable)[sorted_indices]

    return pd.Series(shuffled_variable)


def assert_k_condition(k: int):
    assert k >= 2, "k must be larger or equal to 2"


def check_k_single_variable(vector: pd.Series, k: int):
    assert_k_condition(k)

    counts = vector.value_counts()
    k_check = all(i >= k for i in counts)

    return k_check


def force_k(values: pd.Series, value_type: str, k: int):
    """Shuffle values based on random integers.

    Parameters:
        values (array-like): Input vector of any length
        value_type (str): The type of data, as defined in `data_types.csv`
        k (int): Parameter to fulfill k-anonymity

    Returns:
        pandas.Series: A vector fulfilling k-anonymity
    """
    # convert series into dataframe for better processing
    df = pd.DataFrame({'values': list(values)})
    if value_type in ['date', 'year', 'integer', 'float', 'month']:
        while True:
            number_counts = df['values'].value_counts(dropna=False)
            numbers_to_change = number_counts[number_counts < k].index
            if numbers_to_change.empty:
                break
            numbers_subset = df[df['values'].isin(numbers_to_change)]
            cur_number = numbers_subset['values'].iloc[0]
            nearest_number = find_nearest_number(df['values'].dropna(), cur_number)
            check_nan = True if pd.isnull(cur_number) else False
            if check_nan:
                df.fillna(nearest_number, inplace=True)
            else:
                df.loc[df['values'] == cur_number] = nearest_number
        return pd.Series(df['values'].to_list())
    elif value_type == 'string':
        while not check_k_single_variable(values, k):
            values = [value[:-1] for value in values]
        return pd.Series(values)
    elif value_type == 'category' or value_type == 'alphanumeric':
        k_not_fulfilled = df['values'].value_counts(dropna=False).loc[lambda x: x < k]
        if not k_not_fulfilled.empty:
            if len(k_not_fulfilled) == 1 or k > sum(k_not_fulfilled):
                # if we do have only one category that does not fulfill k,
                # or several categories where their sum does not fulfill k either,
                # we merge these values with the smallest valid category
                # and call it 'Other'
                k_fulfilled = df['values'].value_counts(dropna=False).loc[lambda x: x >= k]
                smallest_valid_category = k_fulfilled.index.tolist()[-1]
                df['Other'] = df['values'].apply(lambda x: x if x in k_fulfilled
                                                 and x is not smallest_valid_category else 'Other')
            else:
                # otherwise, we merge all invalid categories (at least 2) together
                df['Other'] = df['values'].apply(lambda x: x if x not in k_not_fulfilled else 'Other')
            return pd.Series(df['Other'].to_list())
        else:
            return pd.Series(df['values'].to_list())
    else:
        sys.exit(f"Data type {value_type} is not supported.")


def find_nearest_number(values: pd.Series, single_value):
    values = [val for val in values if val != single_value]
    if not values:
        return None
    elif single_value is None:
        return min(values)
    else:
        return min(values, key=lambda x: abs(x - single_value))


def generate_pseudonym(variable: string, length=19):
    if variable in ['VSID']:
        return ''.join(random.choices(POSSIBLE_CHARACTERS, k=length))
    else:
        return ''.join(random.choices(string.digits, k=length))