#!/usr/bin/env bash

read -r -p "Delete result/word_count_per_country? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/word_count_per_country
		hadoop jar \
            ././../../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.train.WordCountPerCountry \
            country/train/AUSTR,country/train/BRAZ,country/train/CANA result/word_count_per_country
            ;;
    *)
	exit 1
	;;
esac
