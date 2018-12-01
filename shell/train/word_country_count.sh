#!/usr/bin/env bash

read -r -p "Delete result/word_class_count? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/word_country_count
		hadoop jar \
            ././../../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.train.WordCountryCount \
            country/train/AUSTR,country/train/BRAZ,country/train/CANA result/word_country_count
            ;;
    *)
	exit 1
	;;
esac
