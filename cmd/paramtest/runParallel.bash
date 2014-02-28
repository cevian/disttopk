#!/bin/bash
if [ "$#" -ne 2 ]; then
	echo "Illegal number params: usage ./runParallel <Parallel> <listsize>"
	exit 0
fi


echo "Starting"
 
for (( c=0; c < $1; c++ ))
do
	./paramtest -suite Distribution -listsize $2 -partition $c -totalpartitions $1 &> Distribution/run.$2.$c.$1.result &
done
 
echo
echo "Started"
wait
echo "Ended"
