"""
Script to download a Netflix dataset from Kaggle, normalize it, 
and load it into a PostgreSQL database.
"""
import logging
import os
import shutil
import kagglehub
import pandas as pd
import sqlalchemy
from sqlalchemy import text

from normalize_table import split_with_foreign_key, split_many_to_many

logging.basicConfig(level=logging.INFO)

# Download latest version
path_to_dataset = kagglehub.dataset_download("shivamb/netflix-shows")

logging.info("Path to dataset files: %s", path_to_dataset)

# Set the dataset directory
logging.info("Setting up dataset directory.")
dataset_dir = os.path.join(os.getcwd(), "datasets")

# Create the directory if it doesn't exist
logging.info("Creating dataset directory at: %s", dataset_dir)
os.makedirs(dataset_dir, exist_ok=True)
# Move the downloaded file to the dataset directory
logging.info("Moving dataset file to: %s", dataset_dir)
shutil.move(path_to_dataset + "\\netflix_titles.csv", dataset_dir)

dataset_path = os.path.join(dataset_dir, "netflix_titles.csv")

# Load the CSV file into a DataFrame
logging.info("Loading dataset from: %s", dataset_path)
df = pd.read_csv(dataset_path)

# Create a new 'id' column and drop the original 'show_id' column
df = df.drop(columns=['show_id'])
df['id'] = df.index + 1

# One-to-many splits
df, type_df = split_with_foreign_key(df, 'type')
df, rating_df = split_with_foreign_key(df, 'rating')

# Many-to-many splits
df, country_df, country_junction = split_many_to_many(df, 'id', 'country', sep=', ')
df, director_df, director_junction = split_many_to_many(df, 'id', 'director', sep=', ')
df, actor_df, actor_junction = split_many_to_many(df, 'id', 'cast', sep=', ')
df, listed_df, listed_junction = split_many_to_many(df, 'id', 'listed_in', sep=', ')

logging.info("Making Final DataFrame adjustments.")
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce').dt.strftime('%Y-%m-%d')
df=df.astype({
    'title': 'string',
    'duration': 'string',
    'description': 'string',
    'date_added': 'datetime64[ns]',
})

df = df[['id', 'type_id', 'title', 'date_added', 'release_year', 'rating_id', 'duration', 'description']]

USERNAME = 'postgres'
PASSWORD = 'postgres'

logging.info("Connecting to PostgreSQL server as user: %s", USERNAME)
engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@localhost:5432/postgres')

DB_NAME = "netflix_titles_db"

logging.info("Checking if database '%s' exists.", DB_NAME)
with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'"))
    exists = result.scalar() is not None

    if not exists:
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
        print(f"Database '{DB_NAME}' created.")
    else:
        print(f"Database '{DB_NAME}' already exists.")

logging.info("Connecting to database: %s", DB_NAME)
db_engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@localhost:5432/{DB_NAME}')

# add the dataframes to sql database
tables = {
    'type': type_df,
    'rating': rating_df,
    'title': df,
    'actor': actor_df,
    'director': director_df,
    'country': country_df,
    'listed_in': listed_df,
    'title_country': country_junction,
    'title_director': director_junction,
    'title_actor': actor_junction,
    'title_listed_in': listed_junction
}

logging.info("Loading tables into the database.")
with db_engine.connect() as conn:
    for name, table_df in tables.items():
        table_df.to_sql(name, conn, if_exists='replace', index=False)
        logging.info("Table '%s' loaded successfully.", name)

# Add primary keys to tables
table_with_primary_key = {
    'type': type_df,
    'rating': rating_df,
    'title': df,
    'actor': actor_df,
    'director': director_df,
    'country': country_df,
    'listed_in': listed_df
}

with db_engine.connect() as conn:
    for name, table_df in table_with_primary_key.items():
        logging.info("Adding primary key to table: %s", name)
        conn.execute(text(f"ALTER TABLE {name} ADD PRIMARY KEY (id)"))
