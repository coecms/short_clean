#!/bin/bash

for project in v45 w35 w40 w42 w48 w97
do 
    for us in `getent group $project | sed 's/.*://'| sed 's/,/ /g'`
    do
        dusql du --mtime=30d /short/$project/$us >> du_$project.out
    done
done

#for us in `getent group w35 | sed 's/.*://'| sed 's/,/ /g'`; do dusql du --mtime=30d /short/w35/$us >> test.out; done