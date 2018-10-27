#!/usr/bin/env bash

read -r -p "Delete result/predict? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/predict
		hadoop jar \
            ././../../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.predict.PredProbability \
            test/AUSTR result/predict
            ;;
    *)
	exit 1
	;;
esac