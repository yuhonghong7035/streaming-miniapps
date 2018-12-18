#!/bin/bash

DATE=`date '+%Y%m%d_%H%M%S'`

mkdir results/${DATE}
mv results/*.csv results/${DATE}/

header=`head -q -n 1 results/${DATE}/*.csv | tail -n 1`

echo ${header} > results/${DATE}/${DATE}.csv

tail -q -n +2 results/${DATE}/*.csv >> results/${DATE}/${DATE}.csv