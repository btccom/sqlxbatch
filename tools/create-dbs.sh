#!/bin/bash

function help() {
    echo "create-dbs.sh <prefix> <cnt>"
    exit 1
}

if [ "${1}" = "" ]; then
    help
fi
if [ "${2}" = "" ]; then
    help
fi

N=$(expr ${2} - 1)
for i in `seq 0 ${N}`; do
    DB="${1}_${i}"
    mysqladmin drop -f ${DB} 2>> /dev/null
    mysqladmin create ${DB} && echo "${DB} created"
done
