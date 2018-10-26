#!/usr/bin/env bash

read -r -p "Delete result/word_dict? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
        hadoop fs -rm -r result/word_dict
		hadoop jar \
            ././../../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.train.WordDict \
            country result/word_dict
            ;;
    *)
	exit 1
	;;
esac


