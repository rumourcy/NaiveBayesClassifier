#!/usr/bin/env bash
hadoop fs -mkdir hdfs:///user/falcon/country
hadoop fs -put ././../../dataset/output/* country
