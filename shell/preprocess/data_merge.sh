#!/usr/bin/env bash

for dir in ././../../dataset/country/train/*
do
	cat ${dir}/* > ././../../dataset/train/`basename ${dir}`
done
