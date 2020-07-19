#!/usr/bin/env bash


set -e


# load conda stuff
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/env/env.sh


# w/o python3-opengl below we have a problem with opengl installation
# (AttributeError: 'NoneType' object has no attribute 'glGetError')
# see https://github.com/MPI-IS/mesh/issues/23
apt-get --yes install python3-opengl


# create a new env so that all the dependencies are not tied to 
# the base environment. 
conda env create --name $CONDA_ENV --file /img/opt/root.yaml


source activate $CONDA_ENV


# install psana
conda install -y mpich=3.3.2 psana-conda=2.0.9 ipython numpy scipy cython matplotlib pandas pillow pyqt  pyqtgraph requests numba


# install experiment db (to support psana1)
conda install -y psana-expdb -c lcls-rhel7


# remove mpi (mpich or openmpi) with --force to leave mpi4py inplace
# this makes sure that mpi4py is compatible with mpich
conda uninstall -y --force mpich
