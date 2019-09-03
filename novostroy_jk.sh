#!/bin/bash

cd `dirname "$0"`

DATE=`date +%Y%m%d`

for CITY in "Moscow" "St.Petersburg"; do
    docker run -v`pwd`:/root/script/:Z -e https_proxy='mwg-tp.msk.vtb24.ru:8080' -e PYTHON_COMMAND="-u novostroy_buildings.py /root/script/jk-${CITY}/ ${CITY} 0" run-python

    ./hadoop.sh fs -put jk-${CITY}/data.tsv /data/adwh/outer_data/novostroy/data-jk-${CITY}-${DATE}.tsv

    rm -rf jk-${CITY}/
done

