# Netflix Dataset Normalization and Database Loader

A Python project that **downloads the Netflix Movies and TV Shows dataset from Kaggle**, **normalizes it into a relational schema**, and **loads it into a PostgreSQL database**.

This project demonstrates:
- Data normalization (1-to-many and many-to-many relationships)
- Database schema creation and population using **SQLAlchemy**
- Automated data ingestion pipelines
- Modular, reusable normalization utilities for pandas DataFrames

---

## Project Structure

```
├── csv_to_database.py       # Main script to orchestrate download, normalize, and load
├── normalize_table.py       # Helper module for one-to-many and many-to-many splits
├── datasets/                # Folder where the dataset will be stored
└── README.md
```

---

## Features

- Normalizes raw CSV data into relational tables
- Handles one-to-many and many-to-many relationships automatically
- Loads normalized data into PostgreSQL using SQLAlchemy
- Reusable normalization functions for other projects
- Creates the database if it doesn’t already exist

---

## Tech Stack

- **Python 3.10+**
- **Pandas**
- **SQLAlchemy**
- **psycopg2**
- **kagglehub** (to download datasets directly from Kaggle)
- **PostgreSQL**

---

## How to Run

### 1. Clone the repository
```bash
git clone https://github.com/Murray-Assal/csv-to-postgresql-injestion.git
cd csv-to-postgresql-injestion
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```


### 3. Configure PostgreSQL
Make sure PostgreSQL is running locally and accessible.  
By default, the script connects using:
```python
USERNAME = 'postgres'
PASSWORD = 'postgres'
DB_NAME = 'netflix_titles_db'
```

You can change these credentials in **csv_to_database.py**.

---

## How It Works

1. **Download** the [Netflix dataset from Kaggle](https://www.kaggle.com/shivamb/netflix-shows) using `kagglehub`.
2. **Normalize** the dataset:
   - One-to-many: `type`, `rating`
   - Many-to-many: `country`, `director`, `cast`, `listed_in`
3. **Create PostgreSQL database** (`netflix_titles_db`) if it doesn’t exist.
4. **Load normalized tables** into PostgreSQL:
   - `type`, `rating`, `title`, `actor`, `director`, `country`, `listed_in`
   - Junction tables: `title_country`, `title_director`, `title_actor`, `title_listed_in`
5. **Add primary keys** to all entity tables.

---

## Example Output

After running the script, your PostgreSQL database will contain a normalized schema like this:

```
type(id, type)
rating(id, rating)
title(id, type_id, title, date_added, release_year, rating_id, duration, description)
actor(id, actor)
director(id, director)
country(id, country)
listed_in(id, listed_in)
title_actor(title_id, actor_id)
title_director(title_id, director_id)
title_country(title_id, country_id)
title_listed_in(title_id, listed_in_id)
```

---

## Reusing the Normalization Utilities

You can reuse the helper functions from `normalize_table.py` in other projects:

```python
from normalize_table import split_with_foreign_key, split_many_to_many

# One-to-many
child_df, parent_df = split_with_foreign_key(df, 'rating')

# Many-to-many
left, right, junction = split_many_to_many(df, 'movie_id', 'genres', sep=', ')
```

---

## Logging

Both scripts use Python’s `logging` module.  
Logs will appear in the terminal, showing progress through downloading, normalization, and loading steps.

---


## Acknowledgements

- Dataset: [Netflix Movies and TV Shows on Kaggle](https://www.kaggle.com/shivamb/netflix-shows)
- Author: Murad Mohamed Abd-El-Motaleb Saleh
