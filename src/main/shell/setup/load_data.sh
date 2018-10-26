#! /bin/bash
hadoop fs -mkdir hdfs:///user/falcon/country
hadoop fs -put output/* country
