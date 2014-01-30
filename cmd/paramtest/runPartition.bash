#!/bin/bash
if [ "$#" -ne 3 ]; then
      echo "Illegal number of parameters: Usage ./runPartition <Suite> <Partition> <TotalPartition>"
      exit 0
fi
if [ ! -d $1 ]; then
      echo "Directory $1 must exist"
      exit 0
fi

go build
./paramtest -suite $1 -partition $2 -totalpartitions $3 2>&1 | tee -a $1/run.$1.$2.$3.$HOSTNAME.log | less
