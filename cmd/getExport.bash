#!/bin/bash
LINE=`cat $@ |grep -n "Start Export"|tail -n 1| cut -d ':' -f1`
LINE=$(($LINE+1))
tail -n +$LINE $@| grep "Export"
