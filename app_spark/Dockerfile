FROM apache/spark:3.5.1

WORKDIR /app

COPY scripts/ ./scripts
COPY data/ ./data

ENV PATH="/opt/spark/bin:${PATH}"

EXPOSE 7077 8080

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]

