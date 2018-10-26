#!/usr/bin/env bash
for dir in ././../../dataset/country/*
do
	cat $dir/* > ././../../dataset/output/`basename $dir`
done
