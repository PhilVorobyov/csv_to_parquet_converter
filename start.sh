#!/usr/bin/env bash

HOST="root@localhost"
FILE="/srv/destinations.csv"

if ssh $HOST -p 2222 "test -e $FILE"; then
    echo "file is already uploaded to virtual box"
else
    echo "uploading csv file virtual box"
    scp -P 2222 /Users/philipvorobyov/devei/workspace/hdfs/src/hdfs/input_data/destinations.csv  $HOST:/srv
    echo "uploading csv to hdfs"
    ssh -t $HOST -p 2222 'sudo hdfs dfs -put /srv/destinations.csv  /user/maria_dev/'
fi

echo "package jar file"
mvn clean package
echo "run application"
java -jar target/hdfs-1.0.jar