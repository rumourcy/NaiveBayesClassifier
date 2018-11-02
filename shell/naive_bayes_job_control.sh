#!/usr/bin/env bash

read -r -p "Delete result/word_dict result/join_word_country result/cond_probability? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/word_dict result/join_word_country result/cond_probability
		hadoop jar \
            ././../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.NaiveBayesTrain \
            country result/word_dict \
            result/join_word_country result/cond_probability
            ;;
    *)
	exit 1
	;;
esac