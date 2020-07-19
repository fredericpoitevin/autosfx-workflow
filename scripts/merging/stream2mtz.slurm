#!/bin/bash -l
#SBATCH --account=lcls
#SBATCH --job-name=arp_example
#SBATCH --nodes=1
#SBATCH --constraint=knl
#SBATCH --time=00:15:00
#SBATCH --image=docker:slaclcls/lcls-py2:latest
#SBATCH --exclusive
#SBATCH --qos=debug

srun -n 1 -c 1 shifter \
    ./stream2mtz.sh  \
      stream:cxic0415.stream  \
      jobname:strep  \
      ucell:strep_more.cell  \
      pgroup:2  \
      sgnum:4  \
      maxadu:13500  \
      wavela:0.969  \
      merge:TRUE  \
      friedel:FALSE  \
      workdir:./  \
      highres:1.5

