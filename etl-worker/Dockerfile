FROM jupyter/pyspark-notebook

USER root
RUN cd /usr/local/spark/jars && wget https://jdbc.postgresql.org/download/postgresql-42.2.14.jar
RUN chmod 777 /usr/local/spark

USER jovyan
COPY ./src/app /app
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade -r /requirements.txt
