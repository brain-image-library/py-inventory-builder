#!/bin/bash

if [ ! -d database ]; then
	mkdir database
fi

singularity instance start --bind $PWD/database:/data/db mongo.sif mongo
