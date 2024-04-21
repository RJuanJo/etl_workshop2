# Data Engineering Workshop 2 #

## Overview ##
For this time, the project consists of automating the appropriate ETL process using Airflow, where these data are stored in a database and Google Drive, to finally create a dashboard with Power BI.
- Docker is used as a tool to run and utilize Apache Airflow on a Windows operating system.
- Google Drive API provided by Google Cloud is used to transfer the processed data to our Google Drive.

## Table of Contents ##
- [Setup](#setup)
- [Data Preparation](#data-preparation)
- [Airflow Automating](#airflow-automating)
- [Visualizations](#visualizations)
  
## Setup <a name="setup"></a> ##

First of all, create a new folder for the project with "logs" and "plugins" folders in it and clone this repository.

You must have installed the following programs:

   - **[Python](https://www.python.org)**
   - **[PostgreSQL](https://www.postgresql.org/download/)**
   - **[PowerBI](https://powerbi.microsoft.com/es-es/downloads/)**
   - **[VS Code](https://code.visualstudio.com/download)** or **[Jupyter](https://jupyter.org/install)**
   - **[Docker](https://www.docker.com/products/docker-desktop/)**
  
**Enable the Google Drive API**

  You need to enable the Google Drive API for this project on Google Cloud Platform:
  
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

  Using the **[requirements.txt](https://github.com/RJuanJo/etl_workshop2/blob/main/config/requirements.txt)** go to the root of the repository and run the   following command in a terminal shell:

  ```python
  pip install -r ../config/requirements.txt
  ```

  Previous command will install the following necessary libraries for the project

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
## Data Preparation <a name="data-preparation"></a> ##
  Before running Airflow, the database must be created, and the data for the Grammys must be stored as requested in the context of the workshop. 
  
  Go to the [db_models](https://github.com/RJuanJo/etl_workshop2/tree/main/db_models) folder and run the [create_db](https://github.com/RJuanJo/etl_workshop2/blob/main/db_models/create_db.py) file, which   will create the database and store the data in the Grammys in its respective table. 
  
  Second, in the [notebooks](https://github.com/RJuanJo/etl_workshop2/tree/main/notebooks) folder where there is an EDA for each file (Spotify and Grammys), this will help better understand the data being handled and provide some visualizations that aid in analysis.

## Airflow Automating <a name="airflow-automating"></a> ##

  Here is where the process of data extraction, transformation, and loading (ETL) is carried out. 
  
  Each process is performed using functions that serve as tasks in our DAG.
  
  ### Airflow explanation ###
    
  In the dags folder, there are two subfolders:
     **dags_connection:** It is responsible for creating the DAG of our Airflow and connecting each of the functions that will serve as tasks within it.
     
  **etls:** Here are the files that contain the functions of our tasks and are divided as follows:   
      - grammy_etl: responsible for the ETL for the Grammy dataset.
      - spotify_etl: responsible for the ETL for the Spotify dataset.
      - merge_load_data: responsible for merging the data returned by the previous files and uploading them to the database in a new table.
  
  ### Run Airflow ### 
        
  To ensure that Docker is running and to initiate your Airflow environment, you can follow these steps:

  Verify Docker is Active:
    Open a terminal and check if Docker containers are running by using the command:
    ```bash
    docker ps
    ```
    
