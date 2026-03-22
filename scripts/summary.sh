#!/bin/bash

/bil/data/9a/d0/9ad0d3df8d000071/1043176282
/bil/data/9a/d0/9ad0d3df8d000071/1043176281
/bil/data/9a/d0/9ad0d3df8d000071/1043176279)

module load anaconda3 

for DIRECTORY in "${DIRECTORIES[@]}"
do
	python ./manifest-builder.py -d $DIRECTORY -n 60 --update --compress
done
