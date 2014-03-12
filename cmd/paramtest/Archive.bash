#!/bin/bash
if [ "$#" -ne 1 ]; then
        echo "Illegal number params: usage ./Archive <index>"
        exit 0
fi
mkdir Alternatives.$1 
mv Alternatives/* Alternatives.$1
mkdir Distribution.$1 
mv Distribution/* Distribution.$1
bash ./exportAll.bash Distribution.$1/*  > /tmp/Distribution.$1.result
bash ./exportAll.bash Alternatives.$1/*  > /tmp/Alternatives.$1.result
