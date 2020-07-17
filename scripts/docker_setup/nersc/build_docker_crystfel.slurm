#!/bin/bash -l
#SBATCH --account=lcls
#SBATCH --job-name=arp_example
#SBATCH --nodes=1
#SBATCH --constraint=knl
#SBATCH --time=00:15:00
#SBATCH --image=docker:slaclcls/lcls-py2:latest
#SBATCH --exclusive
#SBATCH --qos=debug

shifter bash ./build_docker_crystfel.sh

