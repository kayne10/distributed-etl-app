FROM jupyter/pyspark-notebook

COPY spark-master.conf /usr/local/spark/conf/spark-defaults.conf

EXPOSE 7077 8080
CMD [ "spark-class", "org.apache.spark.deploy.master.Master" ]