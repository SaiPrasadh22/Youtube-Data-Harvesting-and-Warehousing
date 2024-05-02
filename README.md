# YouTube Data Harvesting and Warehousing using SQL and Streamlit

This repository contains Python code for a web application built with Streamlit that interacts with the YouTube API, MySQL database, and executes SQL queries to fetch and display YouTube data.

## Overview

The application allows users to fetch and view data related to YouTube channels, videos, and comments. It provides functionalities to execute predefined SQL queries to analyze the data.

## Prerequisites

Before running the application, ensure you have the following dependencies installed:

- Python 3.x
- Google API client library (`googleapiclient`)
- Streamlit (`streamlit`)
- MySQL Connector (`mysql.connector`)
- Pandas (`pandas`)
- SQLAlchemy (`sqlalchemy`)
- Regular Expressions (`re`)

You can install these dependencies using `pip`:

```bash
pip install google-api-python-client streamlit mysql-connector-python pandas sqlalchemy
```

## Getting Started

1. Clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/youtube-data-harvesting.git
```

2. Navigate to the project directory:

```bash
cd youtube-data-harvesting
```

3. Run the Streamlit application:

```bash
streamlit run app.py
```

4. Access the application in your web browser at `http://localhost:8501`.

## Usage

- Upon running the application, you'll be presented with a Streamlit interface.
- You can select different options from the sidebar to view channels, videos, comments, or execute predefined queries.
- For querying, select the desired query from the dropdown and view the results.


## Acknowledgments

- This application was developed as part of a project to demonstrate data harvesting and warehousing techniques using Streamlit, YouTube API, and MySQL.
