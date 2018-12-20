#!/bin/bash

DATE=`date '+%Y%m%d_%H%M%S'`

if [ $# -eq 0 ]
    then
        echo "Create new Result Directory"
        mkdir results/${DATE}
        rm results/latest
        ln -fs ${DATE} results/latest
fi

cp results/*.csv results/latest/

header=`head -q -n 1 results/latest/*.csv | tail -n 1`

echo $header > results/latest/all.csv

tail -q -n +2 results/latest/*.csv >> results/latest/all.csv

