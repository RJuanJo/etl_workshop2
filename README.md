# Data Engineering Workshop 2 #

## Overview ##
_For this time, the project consists of automating the appropriate ETL process using Airflow, where these data are stored in a database and Google Drive, to finally create a dashboard with Power BI._ 
- _Docker is used as a tool to run and utilize Apache Airflow on a Windows operating system._
- _Google Drive API provided by Google Cloud is used to transfer the processed data to our Google Drive.

## Table of Contents ##
- [Setup](#setup)
- [Data Handling](#data-handling)
- [Data Loading](#data-loading)
- [Airflow Automating](#airflow-automating)
- [Visualizations](#visualizations)
  
### Setup <a name="setup"></a> ###

_First of all, create a new folder and clone this repository._

You must have installed the following programs:

   - **[Python](https://www.python.org)**
   - **[PostgreSQL](https://www.postgresql.org/download/)**
   - **[PowerBI](https://powerbi.microsoft.com/es-es/downloads/)**
   - **[VS Code](https://code.visualstudio.com/download)** or **[Jupyter](https://jupyter.org/install)**
   - **[Docker](https://www.docker.com/products/docker-desktop/)**
  
**Enable the Google Drive API**

  First, you need to enable the Google Drive API for this project on Google Cloud Platform:
  
  Go to the Google [Cloud Console](https://console.cloud.google.com/welcome/new).
 
   - **Select or create a project.**

  Go to "APIs & Services" > "Library."
  
  Search for "Google Drive API" and enable it.
  
  - **Create credentials**
  
  To authenticate, you need to create credentials:
  
  Inside the console, go to "Credentials" and choose "Create credentials."
  
  Select "Service account".
  
  Save the JSON credentials file as "secret.json", which will include your access key.

  JSON credentials files ("credentials.json" & "secret.json") with this format into the **[config](https://github.com/RJuanJo/etl_workshop2/tree/main/config)** folder:
  
  credentials.json:
  
  ```
  {
    "user": "your_user",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port"(5432 by deffault),
    "database": "db_name"
  }
  ```

  secret.json
  
  ```
  {
  "type": "service_account",
  "project_id": "[YOUR_PROJECT_ID]",
  "private_key_id": "[YOUR_PRIVATE_KEY_ID]",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[YOUR_PRIVATE_KEY]\n-----END PRIVATE KEY-----\n",
  "client_email": "[YOUR_CLIENT_EMAIL]",
  "client_id": "[YOUR_CLIENT_ID]",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "[YOUR_CLIENT_X509_CERT_URL]",
  "universe_domain": "googleapis.com"
  }
  ```

  _Using the **[requirements.txt](https://github.com/RJuanJo/etl_workshop2/blob/main/config/requirements.txt)** go to the root of the repository and run the   following command in a terminal shell:_

  ```python
  pip install -r ../config/requirements.txt
  ```

  _Previous command will install the following necessary libraries for the project_

  ```python
  - apache-airflow
  - pandas
  - numpy
  - requests
  - psycopg2-binary
  - matplotlib
  - seaborn
  - scipy
  - sqlalchemy
  - google-auth
  - google-auth-oauthlib
  - google-auth-httplib2
  - google-api-python-client
  ```

### Data Loading and Transformation <a name="data-loading"></a> ###
 _This process was carried out in two parts, the **[First Notebook](https://github.com/RJuanJo/etl_workshop1/blob/main/notebooks/load_data.ipynb)** is responsible for reading and loading the data into the MySQL database. These are loaded and stored using a structure or model defined in the **[Structure File](https://github.com/RJuanJo/etl_workshop1/blob/main/db_model/models_structure.py)** inside the **[db_model](https://github.com/RJuanJo/etl_workshop1/tree/main/db_model)** folder._

_The **[Second Notebook](https://github.com/RJuanJo/etl_workshop1/blob/main/notebooks/workshop_eda.ipynb)** is where we extract the data from the database and begin a process of transformation and exploratory data analysis (EDA) to finally store the data in a new table also previously structured in the **[Structure File](https://github.com/RJuanJo/etl_workshop1/blob/main/db_model/models_structure.py)**._


### Visualizations <a name="visualizations"></a> ###

- Hires by Technology (Pie Chart): _Visualization of the distribution of hires by technology which were categorized to have a better format and ease when performing the analysis._

- Hires by Year (Horizontal Bar Chart): _Shows the number of hires per year._

- Hires by Seniority (Bar Chart): _Represents the number of hires by seniority level._

- Hires by Country Over Years (Multiline Chart): _Hiring trends by the chosen countries over the years._

### These visualizations can be seen in the **[Dashboard Summary](https://github.com/RJuanJo/etl_workshop1/blob/main/data/files/Workshop_DashB.pdf)**.
### In turn, you can access the **[Virtual Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNjJjYzhlMzMtMDRiYy00NWRkLTg2ZGQtN2EwNGM2NTMxYjQ5IiwidCI6IjY5M2NiZWEwLTRlZjktNDI1NC04OTc3LTc2ZTA1Y2I1ZjU1NiIsImMiOjR9&pageName=ReportSection)** for a more interactive experience and a greater display of the data.
