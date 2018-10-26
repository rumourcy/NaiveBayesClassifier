#!/usr/bin/env bash

read -r -p "Delete result/word_class_count? [Y/n] " input

case ${input} in
    [yY][eE][sS]|[yY])
		hadoop jar \
            ././../../target/NaiveBayesClassifier-1.0.0.jar \
            io.github.trierbo.train.WordClassCount \
            country result/word_class_count
            ;;
    *)
	exit 1
	;;
esac
