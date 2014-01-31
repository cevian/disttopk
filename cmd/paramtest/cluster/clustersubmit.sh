#!/bin/csh
#
#***
#*** "#PBS" lines must come before any non-blank, non-comment lines ***
#***  submit with qsub -t 0-9 cluster_submit.sh
#
# 1 node, 1 CPU per node (total 1 CPU), wall clock time of 30 hours
#
#PBS -l walltime=01:00:00,nodes=1:ppn=1
#
# merge STDERR into STDOUT file
#PBS -j oe
#
# send mail if the process aborts, when it begins, and
# when it ends (abe)
#PBS -m abe
#PBS -M arye@CS.Princeton.EDU
cd $PBS_O_WORKDIR
/usr/bin/time bash runPartition.bash Distribution $PBS_ARRAYID 10

