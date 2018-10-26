#! /bin/bash
for dir in ./Country/*
do
	cat $dir/* > ./output/`basename $dir`.txt
done
