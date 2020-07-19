#!/usr/bin/env bash


set -e


# load conda stuff
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/env/env.sh
source activate $CONDA_ENV


# install pip packages
pkg_data_dir=$(readlink -f $(dirname ${BASH_SOURCE[0]}))/pkg_data
echo "ADDING PIP PACKAGES TO CONDA ENV $CONDA_ENV"
pip install -r $pkg_data_dir/$PIP

