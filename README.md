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
spark-submit /app/scripts/etl_script.py
