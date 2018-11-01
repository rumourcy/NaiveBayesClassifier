#!/usr/bin/env bash

read -r -p "Delete result? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result
		hadoop jar \
            ././../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.NaiveBayesJobControl \
            country result/word_country_count country result/word_dict \
            result/join_word_country result/cond_probability \
            test/AUSTR,test/BRAZ,test/CANA result/predict
            ;;
    *)
	exit 1
	;;
esac