# LCLS AutoSFX workflows
development and maintenance of (Airflows) workflows for automated SFX processing at LCLS

## Workflow implementation at SLAC:
- DAG design: `/gpfs/slac/cryo/fs1/daq/lcls/dev/airflow/`
- DAG interface: https://lcls-airflow.slac.stanford.edu/admin/

## Workflow tasks
We maintain the scripts corresponding to the individual tasks in another repo: [autosfx-scripts](https://github.com/slaclab/autosfx-scripts)

## File management at NERSC
The task scripts are at `/project/projectdirs/lcls/SFX_automation/`

The current format for experiment directory follows that at SLAC, meaning those two paths should be synced:
- NERSC: `/global/cscratch1/sd/psdatmgr/data/psdm/cxi/<experiment>/scratch` (obsolete: `/project/projectdirs/lcls/exp/cxi/cxic0515/`)
- SLAC: `/reg/d/psdm/cxi/cxic0515/`

## Job management at NERSC

### protocol to setup the whole workflow
- Define workflow in logbook: see `DAG` in `/reg/neh/home5/fpoitevi/sfx_trigger_peak-and-index.sh`
- Start workflow in logbook


### kill a job submitted by the DAG
```bash
ssh psdatmgr@psbuild-rhel7-01
/reg/g/psdm/psdatmgr/tmp/kill_slurm_job.sh  <job id>
```
