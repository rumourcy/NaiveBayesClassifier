#!/usr/bin/env bash

read -r -p "Delete result/word_count_per_country result/word_dict result/word_country_count result/cond_probability? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/word_count_per_country result/word_dict result/word_country_count result/cond_probability
		hadoop jar \
            ././../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.NaiveBayes \
            train \
            result/word_count_per_country result/word_dict \
            result/word_country_count result/cond_probability
            ;;
    *)
	exit 1
	;;
esac