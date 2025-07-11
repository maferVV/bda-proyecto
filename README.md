# cleanup (mostly optional)

docker stop $(docker ps -q)

docker rm $(docker ps -aq)

docker rmi $(docker images -q)

docker volume prune

docker network prune

# compose

docker-compose down
docker-compose up --build -d

# spark bash

sudo docker exec -it spark bash

# see status 
http://localhost:8080/

#test submit
spark-submit /app/scripts/hello_world.py

#etl script
spark-submit /app/scripts/etl_csv.py > /tmp/output.log 2>&1
## read output
cat /tmp/output.log | grep "Number"

## confirm output
ls -l /app/druid_shared


# druid ingest parquet 
curl -X POST http://localhost:8081/druid/indexer/v1/task \
  -H "Content-Type: application/json" \
  -d @app_druid/parquet_ingest.json





# prompt
My current project structure looks like so:

```

.
└── mafer_temp/
    ├── docker-compose.yaml
    └── app_spark/
        ├── Dockerfile
        ├── scripts/
        │   ├── etl_script.py
        │   └── hello_world.py
        └── data/
            ├── yellow_tripdata_2025-01.parquet
            └── ssps.csv
```

the `docker-compose.yaml` looks like this:

```
volumes:
  # metadata_data: {}
  middle_var: {}
  historical_var: {}
  broker_var: {}
  coordinator_var: {}
  router_var: {}
  druid_shared: {}

services:
  spark:
    build:
      context: ./app_spark  # Specify the context for the build
    container_name: spark
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "7077:7077"
      - "8080:8080"
    volumes:
      - ./app_spark/:/app/ 
      - druid_shared:/app/druid_shared  # Mount the druid_shared volume

  # Spark Worker
  worker:
    image: docker.io/bitnami/spark:3.3
    #build:
    #  context: ./app_spark  # Specify the context for the build
    container_name: worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
    depends_on:
      - spark
    ports:
      - "8084:8084"
    volumes:
      - ./app_spark/:/app/ 
      - druid_shared:/app/druid_shared  # Mount the druid_shared volume
```

app_spark/Dockerfile:

```
FROM docker.io/bitnami/spark:3.3

# Switch to root user to create a new user
USER root
```

and `/app_spark/scripts/hello_world.py`:

```
from pyspark.sql import SparkSession

# Update the master URL to use the correct service name
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .master("spark://spark:7077") \
    .getOrCreate()

# Simple Spark job
data = ["Hello", "World!", "This", "is", "Spark!"]
rdd = spark.sparkContext.parallelize(data)
result = rdd.collect()
for word in result:
    print(word)

spark.stop()
```

the file /app_spark/data/ssps.csv starts like this:

```
﻿Estado Pobreza,Sexo,Provincia,Canton,Distrito,Recuento de snb003_id_persona
,,,,,6614
,Desconocido,,,,15014
,Hombre,,,,491059
,Mujer,,,,467077
,No indica,,,,117
,Persona intersex,,,,138
No Pobre,Hombre,GUANACASTE,ABANGARES,COLORADO,759
No Pobre,Mujer,GUANACASTE,ABANGARES,COLORADO,752
No Pobre,Hombre,GUANACASTE,ABANGARES,LA SIERRA,495
No Pobre,Mujer,GUANACASTE,ABANGARES,LA SIERRA,482
No Pobre,,GUANACASTE,ABANGARES,LAS JUNTAS,2
No Pobre,Hombre,GUANACASTE,ABANGARES,LAS JUNTAS,1819
No Pobre,Mujer,GUANACASTE,ABANGARES,LAS JUNTAS,1772
No Pobre,Hombre,GUANACASTE,ABANGARES,SAN JUAN,221
No Pobre,Mujer,GUANACASTE,ABANGARES,SAN JUAN,208
No Pobre,Hombre,SAN JOSE,ACOSTA,CANGREJAL,270
No Pobre,Mujer,SAN JOSE,ACOSTA,CANGREJAL,218
No Pobre,,SAN JOSE,ACOSTA,GUAITIL,1
No Pobre,Hombre,SAN JOSE,ACOSTA,GUAITIL,583
... etc
```

Based on the given information, please help me create a script called `etl_csv.py`. It should be able to:
* read ssps.csv
* apply basic ETL modifications to the file
* save the output as /data/ssps_modified.csv

