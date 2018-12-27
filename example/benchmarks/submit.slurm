#!/bin/sh
#------------------Scheduler Options--------------------
#SBATCH -J Pilot-Streaming          # Job name
#SBATCH -N 1                   # Total number of nodes (24 cores/node)
#SBATCH -n 1                  # Total number of tasks
#SBATCH -p normal              # Queue name
#SBATCH -o pilot-streaming.o%j      # Name of stdout output file (%j expands to jobid)
#SBATCH -t 48:00:00            # Run time (hh:mm:ss)
#------------------General Options---------------------
export TACC_LAUNCHER_PPN=24

cd $HOME/notebooks/streaming-miniapps/example/benchmarks; python masa_lambda_benchmark.py
