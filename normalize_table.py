"""
Module to normalize pandas DataFrames by splitting one-to-many and many-to-many relationships
into separate tables with foreign key references.
"""
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

def split_with_foreign_key(df, key_col):
    """
    Splits a one-to-many relationship into parent and child tables
    Args:
        df (pd.DataFrame): The original DataFrame.
        key_col (str): The column name to split on.
    Returns:
        child (pd.DataFrame): The child DataFrame with foreign key.
        parent (pd.DataFrame): The parent DataFrame with unique entries.
    """

    logging.info("Validating input DataFrame and column: %s", key_col)
    # Input validation
    if isinstance(df, pd.DataFrame) is False:
        raise TypeError("Input must be a pandas DataFrame.")
    if df.empty:
        raise ValueError("DataFrame is empty.")
    if len(df) < 2:
        raise ValueError("DataFrame must have at least two rows to split.")
    if key_col not in df.columns:
        raise ValueError(f"Column '{key_col}' not found in DataFrame.")
    if df[key_col].dtype not in [object, str]:
        raise ValueError(f"Column '{key_col}' must be of string type for this operation.")
    if df[key_col].isnull().all():
        raise ValueError(f"Column '{key_col}' contains only null values.")
    if df[key_col].nunique() == len(df):
        raise ValueError(f"Column '{key_col}' has all unique values; no one-to-many relationship to split.")
    if df[key_col].nunique() == 1:
        raise ValueError(f"Column '{key_col}' has only one unique value; no one-to-many relationship to split.")
    if type(df[key_col].iloc[0]) in [list, set, tuple]:
        raise ValueError(f"Column '{key_col}' contains multivalued entries; many-to-many relationship is more appropriate.")

    # Create parent table with unique values
    logging.info("Splitting DataFrame on column: %s", key_col)
    parent = df[[key_col]].drop_duplicates().reset_index(drop=True)
    parent = parent.astype({key_col: 'string'})
    parent['id'] = parent.index + 1

    # Handle missing values by assigning 'Unknown'
    logging.info("Handling missing values in column: %s", key_col)
    df[key_col] = df[key_col].fillna('Unknown')
    parent[key_col] = parent[key_col].fillna('Unknown')


    # Create child table with foreign key reference
    logging.info("Creating child DataFrame with foreign key reference to parent.")
    child = df.merge(parent, on=key_col, suffixes=("", "_parent")).rename(columns={'id_parent': f'{key_col}_id'})

    logging.info("Returning child and parent DataFrames.")
    return child.drop(columns=[key_col]), parent

def split_many_to_many(df, left_col, right_col, sep=None):
    """
    Splits a DataFrame with a many-to-many relationship between two columns
    (including multivalued fields) into three DataFrames:
    - left: unique entities of left_col
    - right: unique entities of right_col
    - junction: mapping table with foreign keys

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame.
    left_col : str
        The column representing the left entity (e.g., 'student_name').
    right_col : str
        The column representing the right entity (e.g., 'course_name').
    sep : str or None, optional
        If the right_col contains strings with multiple values (e.g. "Math,History"),
        specify the separator (e.g. ","). If None, assumes lists.
    """

    logging.info("Validating input DataFrame and columns: %s, %s", left_col, right_col)
    # Input validation
    if isinstance(df, pd.DataFrame) is False:
        raise TypeError("Input must be a pandas DataFrame.")
    if df.empty:
        raise ValueError("DataFrame is empty.")
    if len(df) < 2:
        raise ValueError("DataFrame must have at least two rows to split.")
    if left_col not in df.columns:
        raise ValueError(f"Column '{left_col}' not found in DataFrame.")
    if right_col not in df.columns:
        raise ValueError(f"Column '{right_col}' not found in DataFrame.")
    if df[left_col].isnull().all():
        raise ValueError(f"Column '{left_col}' contains only null values.")
    if df[right_col].isnull().all():
        raise ValueError(f"Column '{right_col}' contains only null values.")
    if df[right_col].nunique() == len(df):
        raise ValueError(f"Column '{right_col}' has all unique values; no many-to-many relationship to split.")

    # 1. Normalize right_col into lists if it’s a delimited string
    logging.info("Normalizing column '%s' with separator: %s", right_col, sep)
    if sep is not None:
        df = df.copy()
        df[right_col] = df[right_col].apply(
            lambda x: [v.strip() for v in x.split(sep)] if isinstance(x, str) else x
        )

    # 2. Explode multi-valued records into multiple rows
    logging.info("Exploding column '%s' into multiple rows.", right_col)
    df_exploded = df.explode(right_col).reset_index(drop=True)

    # Build left table: keep all columns except the multivalued one
    logging.info("Building left table excluding column: %s", right_col)
    left_columns = [c for c in df.columns if c != right_col]
    left = df[left_columns].drop_duplicates().reset_index(drop=True)

    # Build right table: distinct right_col values
    logging.info("Building right table with unique values from column: %s", right_col)
    right = (
        df_exploded[[right_col]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    right['id'] = right.index + 1
    right = right.astype({right_col: 'string'})
    right.loc[right[right_col].isnull(), right_col] = 'Unknown'

    # 4. Build junction table
    logging.info("Building junction table between '%s' and '%s'.", left_col, right_col)
    junction = (
        df_exploded
        .merge(left, on=left_col)
        .merge(right, on=right_col, suffixes=("_left", "_right"))
        [['id_left', 'id_right']]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={'id_right': f'{right_col}_id', 'id_left': 'title_id'})
    )

    logging.info("Returning left, right, and junction DataFrames.")
    return left, right, junction
