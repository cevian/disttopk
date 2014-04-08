#!/bin/bash
if [ "$#" -ne 1 ]; then
        echo "Illegal number params: usage ./Archive <index>"
        exit 0
fi
mkdir UCB.$1 
mv UCB/* UCB.$1
mkdir WC.$1 
mv WC/* WC.$1
bash ./exportAll.bash WC.$1/*  > /tmp/WC.$1.result
bash ./exportAll.bash UCB.$1/*  > /tmp/UCB.$1.result
