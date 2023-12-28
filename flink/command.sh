sudo docker-compose up  -d
mvn clean package

sudo docker exec -it flink_jobmanager_1 /bin/bash


# chmod 777 /files/

docker-compose down

./bin/flink run -c es.upm.cloud.flink.Exercise1 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise1.csv

./bin/flink run -c es.upm.cloud.flink.Exercise2 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise2.csv

./bin/flink run -c es.upm.cloud.flink.Exercise3 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise3.csv

 ./bin/flink run -c es.upm.cloud.flink.Exercise4 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise4.csv

./bin/flink run -c es.upm.cloud.flink.Exercise5 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise5.csv

./bin/flink run -c es.upm.cloud.flink.Exercise5b /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise5b.csv

./bin/flink run -c es.upm.cloud.flink.Exercise6 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise6.csv

 ./bin/flink run -c es.upm.cloud.flink.Exercise6b /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise6b.csv

./bin/flink run -c es.upm.cloud.flink.Exercise7 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise7.csv
