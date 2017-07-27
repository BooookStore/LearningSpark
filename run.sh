#!/bin/bash

echo "TARGET CLASS ->" $1
echo "RESOURCE ->" ${@:2}

sbt clean package

$SPARK_HOME/bin/spark-submit --class $1 ${@:2}
