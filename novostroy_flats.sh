#!/bin/bash

cd `dirname "$0"`

DATE=`date +%Y%m%d`

mkdir -p cache

for SITE in https://www.novostroy.ru https://www.novostroy.su https://kaluga.novostroy.su https://kaliningrad.novostroy.su https://novosibirsk.novostroy.su; do
    SUFFIX=`echo $SITE | awk -F'/' '{ print $NF; }'`

    docker run -v`pwd`:/root/script/:Z -e https_proxy='mwg-tp.msk.vtb24.ru:8080' -e PYTHON_COMMAND="-u script_hdp.py $SITE /root/script/cache/ 0" run-python

    ./hadoop.sh fs -put cache/novostroy_data_flats.csv /data/adwh/outer_data/novostroy/data-flats-${SUFFIX}-${DATE}.csv

    rm -rf cache/*
done
