#!/usr/bin/env bash


set -e


# load conda stuff
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/env/env.sh


# w/o python3-opengl below we have a problem with opengl installation
# (AttributeError: 'NoneType' object has no attribute 'glGetError')
# see https://github.com/MPI-IS/mesh/issues/23
apt-get --yes install python3-opengl


# create a new env so that all the dependencies are not tied to 
# the base environment. We also need to install mpich here (just
# to force remove it later). The correct mpich gets picked up
# from LD_LIBRARY_PATH=/opt/udiImage/modules/mpich/lib64 when
# shifter images are pulled at Nersc.
pushd /img
    wget -O extras.yaml $EXTRA_YAML
    sed -i '/python=/d' extras.yaml
    sed -i '/psana/d' extras.yaml
    echo "  - mpich=3.3.2" >> extras.yaml
    echo "  - $PSANA_PKG_NAME=$PSANA_VERSION" >> extras.yaml
popd
conda env create --name $CONDA_ENV python=$PYVER --file /img/extras.yaml
rm /img/extras.yaml


source activate $CONDA_ENV


# install experiment db (to support psana1)
conda install psana-expdb -c lcls-rhel7


# remove mpi (mpich or openmpi) with --force to leave mpi4py inplace
# this makes sure that mpi4py is compatible with mpich
conda uninstall -y --force mpich

