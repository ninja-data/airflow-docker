FROM apache/airflow:2.9.3

COPY requirements.txt /requirements.txt

# Upgrade pip
RUN pip install --upgrade pip
# RUN pip install --user --upgrade pip

# Install requirements
RUN pip install --no-cache-dir -r /requirements.txt
# RUN pip install --no-cache-dir --user -r /requirements.txt

# RUN sudo apt install unixodbc-dev

RUN pip install apache-airflow-providers-odbc

# Install the MSSQL provider
RUN pip install apache-airflow-providers-microsoft-mssql

