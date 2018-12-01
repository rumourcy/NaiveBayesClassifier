#!/usr/bin/env bash

hadoop fs -rm -r country
hadoop fs -rm -r train
hadoop fs -mkdir hdfs:///user/falcon/country
hadoop fs -mkdir hdfs:///user/falcon/train
hadoop fs -put ././../../dataset/country/* country
hadoop fs -put ././../../dataset/train/* train
