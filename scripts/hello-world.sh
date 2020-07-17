#!/bin/bash -l
#SBATCH --account=lcls
#SBATCH --job-name=arp_example
#SBATCH --nodes=1
#SBATCH --constraint=knl
#SBATCH --time=00:15:00
#SBATCH --image=docker:slaclcls/lcls-py2:latest
#SBATCH --exclusive
#SBATCH --qos=debug
# This is the entry point into the user code.
# We are launching a shifter image passing in a script that sets up the enviroment inside the shifter image
# The full path to this what we'd register in the UI in workflow definitions
# Note that this sbatch should be the very last command in this script
# It prints the jobid to stdout which is then parsed to determine the SLURM job id
srun -n 1 -c 1 shifter ./hello.sh
