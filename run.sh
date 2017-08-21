#!/bin/bash

jarPath=target/scala-2.10/learningspark_2.10-1.0.jar

echo "TARGET CLASS ->" $1
echo "RESOURCE ->" ${@:2}

rm -r Result

sbt clean package

$SPARK_HOME/bin/spark-submit --class $1 $jarPath ${@:2}
