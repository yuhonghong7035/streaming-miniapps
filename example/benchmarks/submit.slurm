#!/bin/sh
#------------------Scheduler Options--------------------
#SBATCH -J Pilot-Streaming          # Job name
#SBATCH -N 1                   # Total number of nodes (24 cores/node)
#SBATCH -n 1                  # Total number of tasks
#SBATCH -p normal              # Queue name
#SBATCH -o Parametric.o%j      # Name of stdout output file (%j expands to jobid)
#SBATCH -t 48:00:00            # Run time (hh:mm:ss)
#------------------General Options---------------------
export TACC_LAUNCHER_PPN=24

jupyter nbconvert --execute $HOME/notebooks/streaming-miniapps/example/benchmarks/consumer_benchmark.ipynb
