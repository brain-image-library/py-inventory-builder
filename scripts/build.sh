#!/bin/bash

IMAGE=mongo.sif
DEFINITION=Singularity

singularity build $IMAGE $DEFINITION
