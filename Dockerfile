FROM astrocrpublic.azurecr.io/runtime:3.1-5

USER root

# Install ODBC dependencies + Microsoft SQL Server ODBC Driver
RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https software-properties-common && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y \
        msodbcsql18 \
        unixodbc \
        unixodbc-dev \
        libodbc1 \
        odbcinst && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-mysql \
    apache-airflow-providers-postgres \
    apache-airflow-providers-databricks \
    apache-airflow-providers-snowflake \
    pyodbc

USER astro