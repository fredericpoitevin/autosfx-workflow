#!/usr/bin/env bash


# don's stop if errors => this is supposed to be sourced by the main environment
# set -e


# load site-specific variables:
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/vars.sh


# prepend local conda install
if [[ -e $(readlink -f $(dirname ${BASH_SOURCE[0]}))/../../conda.local/env.sh ]]; then
    source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/../../conda.local/env.sh
fi


