#!/usr/bin/env bash

read -r -p "Delete result/word_count_per_country result/word_dict result/word_country_count
result/cond_probability result/country_count result/country_probability result/country_predict? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/word_count_per_country result/word_dict \
            result/word_country_count result/cond_probability \
            result/country_count result/country_probability \
            result/country_predict
		hadoop jar \
            ././../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.NaiveBayes \
            train country/train/BRAZ,country/train/CANA \
            result/word_count_per_country result/word_dict \
            result/word_country_count result/cond_probability \
            result/country_count result/country_probability \
            country/test/BRAZ,country/test/CANA result/country_predict
            ;;
    *)
	exit 1
	;;
esac