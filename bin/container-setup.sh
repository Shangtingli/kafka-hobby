# Start all docker containers
## docker start $(docker ps -q)


# Stop all docker containers
## docker stop $(docker ps -q)

# Remove all exited docker containers
## docker rm $(docker ps -a -f status=exited -q)


# Remove all images
## docker rmi $(docker images -a -q)



# Make sure your docker container is running
docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=`docker-machine ip bigdata` -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka
docker run -d -p 7199:7199 -p 9042:9042 -p 9160:9160 -p 7001:7001 --name cassandra cassandra:3.7


