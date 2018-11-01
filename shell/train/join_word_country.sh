#!/usr/bin/env bash

read -r -p "Delete result/join_word_country? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/join_word_country
		hadoop jar \
            ././../../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.train.JoinWordCountry \
            result/word_dict result/join_word_country
            ;;
    *)
	exit 1
	;;
esac