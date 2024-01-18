
# Set Up
## Deploy docker

```bash
sudo docker-compose up  -d
sudo docker exec -it flink_jobmanager_1 /bin/bash
```

## Local Directory 

Compile Java
```bash
cd ./flink-examples
mvn clean package
```

nc -lk 9999  Up a port in localhost:9999

## Problems with access
In side docker:
```bash
chmod 777 /files/
```

## Ex1
 ```bash
docker exec -it flink_jobmanager_1 /bin/bash
 ./bin/flink run -c es.upm.cloud.flink.sensors.basics.Exercise1 /flink-examples/target/flink-examples-0.1.jar --input /files/sensors/sensorData.csv --output /files/sensors/exercise1.csv
 ```

## Ex2
```bash
docker exec -it flink_jobmanager_1 /bin/bash
./bin/flink run -c es.upm.cloud.flink.sensors.basics.Exercise2 /flink-examples/target/flink-examples-0.1.jar --input /files/sensors/sensorData.csv --output /files/sensors/exercise2.csv

```

./bin/flink run -c es.upm.cloud.flink.sensors.basics.Exercise3 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise3.csv

 ./bin/flink run -c es.upm.cloud.flink.sensors.basics.Exercise4 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise4.csv

./bin/flink run -c es.upm.cloud.flink.sensors.basics.Exercise5 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise5.csv

./bin/flink run -c es.upm.cloud.flink.sensors.basics.Exercise5b /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise5b.csv

./bin/flink run -c es.upm.cloud.flink.sensors.windows.Exercise6 /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise6.csv

 ./bin/flink run -c es.upm.cloud.flink.sensors.windows.Exercise6b /flink-examples/target/flink-examples-0.1.jar --input /files/sensorData.csv --output /files/exercise6b.csv


## Ex7
```bash
docker exec -it flink_jobmanager_1 /bin/bash
./bin/flink run -c es.upm.cloud.flink.sensors.windows.Exercise7 /flink-examples/target/flink-examples-0.1.jar --input /files/sensors/sensorData.csv --output /files/sensors/exercise7.csv

```

## Ex7b
```bash
docker exec -it flink_jobmanager_1 /bin/bash
./bin/flink run -c es.upm.cloud.flink.sensors.windows.Exercise7b /flink-examples/target/flink-examples-0.1.jar --input /files/sensors/sensorData.csv --output /files/sensors/exercise7b.csv

```

## Ex8
```bash
docker exec -it flink_jobmanager_1 /bin/bash
./bin/flink run -c es.upm.cloud.flink.sensors.windows.Exercise8 /flink-examples/target/flink-examples-0.1.jar --input /files/sensors/sensorData.csv --output /files/sensors/exercise8.csv

```
```bash
docker exec -it flink_jobmanager_1 /bin/bash
 ./bin/flink run -c es.upm.cloud.flink.sensors.windows.Exercise8b /flink-examples/target/flink-examples-0.1.jar --input /files/sensors/sensorData.csv --output /files/sensors/exercise8b.csv
```


# Exams

## Jan 2023

```bash
docker exec -it flink_jobmanager_1 /bin/bash
 ./bin/flink run -c es.upm.cloud.flink.exams.Jan2023A /flink-examples/target/flink-examples-0.1.jar --input /files/exams/cars.csv --output /files/exams/jan2023.csv

```
```bash
docker exec -it flink_jobmanager_1 /bin/bash
 ./bin/flink run -c es.upm.cloud.flink.exams.Jan2023B /flink-examples/target/flink-examples-0.1.jar --input /files/exams/cars.csv --output /files/exams/jan2023B.csv

```

```bash
docker exec -it flink_jobmanager_1 /bin/bash
 ./bin/flink run -c es.upm.cloud.flink.exams.Jan2023C /flink-examples/target/flink-examples-0.1.jar --input /files/exams/cars.csv --output /files/exams/jan2023C.csv

```

# Stop docker
```bash
docker-compose down
```
