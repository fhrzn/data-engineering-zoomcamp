FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# define working directory in the image
# so the initial directory of the image would be in /app
# hence the pipeline would be copied there too
WORKDIR /app
COPY ingest_data.py ingest_data.py

# command that will be executed once the image started
ENTRYPOINT [ "python", "ingest_data.py" ]