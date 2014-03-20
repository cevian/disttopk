#!/bin/bash
#/usr/bin/time bash runParallel.bash 15 1000 Alternatives -recordsize=100 -highzipf
/usr/bin/time bash runParallelNew.bash 15 1000 Alternatives 15 -recordsize=100
#/usr/bin/time bash runParallel.bash 15 1000 Distribution -recordsize=10 -highzipf
/usr/bin/time bash runParallelNew.bash 15 1000 Distribution 15 -recordsize=10 -highzipf
/usr/bin/time bash runParallelNew.bash 15 1000 Distribution 15 -recordsize=100
/usr/bin/time bash runParallelNew.bash 15 10000 Distribution 15 -recordsize=100
/usr/bin/time bash runParallelNew.bash 15 100000 Distribution 15 -recordsize=100
/usr/bin/time bash runParallelNew.bash 1000 1000000 Distribution 4 -recordsize=100 -highzipf
