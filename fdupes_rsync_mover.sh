#!/bin/bash

DUPLICATES=../duplicates
if [ ! -d "$DUPLICATES" ]; then
        mkdir $DUPLICATES
fi

IFS=$'\n'
for i in $(fdupes -rf ./); do \
        echo $i; \
        rsync -R $i $DUPLICATES; \
        rm $i; \
        done
echo completed

