#!/bin/bash
if [ "$#" -lt 3 ]; then
	echo "Illegal number params: usage ./runParallel <totoalPartitions> <listsize> <suite> <simultaneous> [otherOptions]"
	exit 0
fi


echo "Starting"

rm cmds.list
str=""
for (( c=0; c < $1; c++ ))
do
	next="./paramtest -suite $3 -listsize $2 -partition $c -totalpartitions $1 ${@:5} &> Distribution/run.$2.$c.$1.`date +%s`.result " 
	#inext=`echo $next\n`
	str=$str$next$'\n'
done
echo "$str" >>cmds.list
 
echo
echo "Started generating list"
echo "Ended generating list"
cat cmds.list | parallel -j $4
