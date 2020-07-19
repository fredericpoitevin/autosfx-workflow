#!/usr/bin/env bash


# stop running if there's an error
set -e


# load site-specific variables:
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/vars.sh


# access "our conda"
if [[ -e $(readlink -f $(dirname ${BASH_SOURCE[0]}))/../../conda.local/env.sh ]]; then
    source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/../../conda.local/env.sh
fi


# check if already installed
# this will only match packages in the current env root
env_grep=$(conda env list | tr '/' 'x' | grep -w $CONDA_ENV  || true)
# don't match paths        ^^^^^^^^^^^^       ^^
if [[ -n $env_grep ]]; then
    echo "$CONDA_ENV already exists... exiting"
    exit
fi


# SKIP: update conda - we'll create a new env which should pull in new stuffs
conda update -c conda-forge --all

# add channels strictly in this order
for (( i=0; i<${#CONDA_CH[@]}; ++i )); do
    conda config --add channels ${CONDA_CH[$i]}
done

echo ""
