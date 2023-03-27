FROM apache/airflow:2.5.2
COPY requirements.txt /requirements.txt
COPY credentials.json /app/credentials_web.json
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt