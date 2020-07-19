#!/usr/bin/env bash


# stop when there's an error
set -e


# load conda stuff
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/env/env.sh


# get the directory of this pipeline
my_dir=$(dirname ${BASH_SOURCE[0]})
pipeline_dir=$(readlink -f $my_dir)


# generate a local env

cat > $pipeline_dir/env.local <<EOF
#
# update PATH (this is local to the current machine)
# Automaticall generated using setup_xtc.sh
#


# load site-specific variables: XTC_**
source $(readlink -f $(dirname ${BASH_SOURCE[0]}))/env/vars.sh


# load environment
source $pipeline_dir/env/env.sh


# activate the work env
source activate $CONDA_ENV


EOF

