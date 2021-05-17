#!/bin/bash

cd /home/00_PROJECTS/001_ALGO_PORTFOLIO/003_DASK_DISTRIBUTED
##
#$WORKING_DIR='dask-worker-space/'
if [ -d "$dask-worker-space/" ]; then rm -Rf $dask-worker-space/; fi
##
source ~/.bashrc
echo 'in directory'
dask-scheduler > dask_scheduler.log 2>&1 &
dask-worker 172.17.0.2:8786 > dask_worker.log 2>&1 --nthreads 1 --nprocs 3 &
##
echo 'dask scheduler: ok'
echo 'dask-worker: ok'
python3 Delayed_20_DASK_Script_Clustering_MonteCarlo.py > report.log 2>&1 && pkill -9 python3 && pkill -9 dask-scheduler && pkill -9 dask-worker
