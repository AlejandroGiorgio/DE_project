# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Airflow
RUN pip install apache-airflow

# Copy the ETL script into the scripts folder
COPY scripts/refreshing.py /app/scripts

# Copy the DAG file into the dags folder of Airflow
COPY dags/my_dag.py /usr/local/airflow/dags

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV AIRFLOW_HOME=/usr/local/airflow

# Run airflow webserver when the container launches
CMD ["airflow", "webserver", "-p", "8080"]