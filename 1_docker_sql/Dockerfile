FROM python:3.11

RUN apt-get install wget
RUN pip3 install pandas sqlalchemy psycopg2-binary

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]