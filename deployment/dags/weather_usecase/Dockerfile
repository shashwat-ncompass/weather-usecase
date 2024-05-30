FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y default-mysql-client python3-pip

# Copy the custom Airflow package
COPY custom_airflow /opt/custom_airflow

# Change ownership of the custom_airflow directory to the airflow user
RUN chown -R airflow:root /opt/custom_airflow

USER airflow

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt --verbose

# Install the custom Airflow package
RUN pip install /opt/custom_airflow

WORKDIR /opt/airflow/dags

EXPOSE 8080