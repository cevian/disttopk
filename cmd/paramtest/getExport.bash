#!/bin/bash
LINE=`cat run_log.3 |grep -n "Start Export"|tail -n 1| cut -d ':' -f1`
LINE=$(($LINE+1))
tail -n +$LINE run_log.3| grep "Export"
