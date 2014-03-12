#!/bin/bash
if [ "$#" -lt 2 ]; then
	echo "Illegal number params: usage ./runParallel <Parallel> <listsize> <suite> <other options>"
	exit 0
fi


echo "Starting"
 
for (( c=0; c < $1; c++ ))
do
	./paramtest -suite $3 -listsize $2 -partition $c -totalpartitions $1 "${@:4}" &> $3/run.$2.$c.$1.`date +%s`.result &
done
 
echo
echo "Started"
wait
echo "Ended"
