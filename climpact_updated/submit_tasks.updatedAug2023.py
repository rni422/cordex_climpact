#!/g/data/hh5/public/apps/nci_scripts/python-analysis3
# Copyright 2021 Scott Wales
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import intake
import os
import subprocess
import shlex

# cat = intake.cat.nci.esgf.cordex
cat = intake.open_esm_datastore('/g/data/w40/ri9247/josh/catalogue.json')

# Directory to store outputs in (also change the `list_*.sh` scripts to match so they can see the outputs)
output_dir = "/g/data/w40/yl1269/climpact_indices/cordex_AUS44i_Aug2023_all_indices"

# Cordex search terms. Possible keys are:
#    project
#    product
#    domain
#    institute
#    driving_model
#    experiment
#    ensemble
#    rcm_name
#    rcm_version
#    time_frequency
# Files will be found from /g/data/rr3/publications/CORDEX and /g/data/al33/replicas/CORDEX

matches = cat.search(
    project="cordex",
    domain=["AUS-44i"],
    experiment=["historical", "rcp45", "rcp85"],
    time_frequency="day",
    variable=["pr", "tasmin", "tasmax"],
)


def submit_runs(df, task="climpact"):
    # Drop 'variable' and 'version'
    groupby_attrs = matches.esmcat.aggregation_control.groupby_attrs[:-2]

    for name, group in df.groupby(groupby_attrs):
        runid = ".".join(name)
        keys = {k: v for k, v in zip(groupby_attrs, name)}
    
        # Name of the PBS job
        pbs_name = f"{task}.{keys['domain']}.{keys['driving_model']}.{keys['experiment']}.{keys['rcm_name']}"

        # Work out the output filename, grabbing the dates of the first and last input files
        r1 = group.iloc[0]
        basename = os.path.basename(r1.path)
        parts = os.path.splitext(basename)[0].split('_')
        t0 = parts[-1].split('-')[0]
        t1 = os.path.splitext(os.path.basename(group.iloc[-1].path))[0].split('_')[-1].split('-')[1]
        parts[-1] = f"{t0}-{t1}"
        file_template = '_'.join(parts[1:])
        input_dir = r1.path.rsplit('/',1)[0]
        #print(file_template)

        # Each variable's Intake ID
        # Aug 15, 2023 - RI makes this a try and except to account for models missing pr or temp variables
        try:
            pr = list(matches.search(variable="pr", **keys).keys())[0]
            tasmin = list(matches.search(variable="tasmin", **keys).keys())[0]
            tasmax = list(matches.search(variable="tasmax", **keys).keys())[0]
        except:
            pass

        # Skip already processed files
        if os.path.exists(os.path.join(output_dir, runid, f"{task}.done")):
            continue

        # Skip runs that are in the queue, or have failed
        if os.path.exists(os.path.join(output_dir, runid, f"{task}.waiting")):
            print(f"Not finished {task} {runid}")
            continue


        if task == "climpact":
            # Is the threshold run done?

            # Name of the matching historical run
            hist_runid = list(name)
            hist_runid[groupby_attrs.index("experiment")] = "historical"
            hist_runid = ".".join(hist_runid)

            # Thresholds filename
            thresholds = os.path.join(output_dir, hist_runid, 'thresholds.nc')

            # historical run itself doesn't need the thresholds
            if hist_runid == runid:
                thresholds = ''

            # Skip non-historical runs that don't have thresholds available
            elif not os.path.exists(os.path.join(output_dir, hist_runid, "thresholds.done")):
                print(f"Waiting on thresholds {runid} {hist_runid}")
                continue

        else: # task == "thresholds"
            thresholds = ''

        # Submit the job
        print(f"Submitting {task} {runid}")
        os.makedirs(os.path.join(output_dir, runid), exist_ok=True)
        command = [
            "/opt/pbs/default/bin/qsub",
            "-N", pbs_name,
            "-v",
            ','.join([f"pr={pr}",
            f"tasmin={tasmin}",
            f"tasmax={tasmax}",
            f"name={runid}",
            f"task={task}",
            f"input_dir={input_dir}",
            f"output_dir={output_dir}/{runid}",
            f"file_suffix={file_template}",
            f"thresholds={thresholds}"]),
            "-o", f"{output_dir}/{runid}/{task}.log",
            f"run_climpact.pbs",
        ]

        with open(os.path.join(output_dir, runid, f"{task}.waiting"), 'w') as f:
            subprocess.run(command, check=True, stdout=f)


df = matches.df


historical = df[df.experiment == "historical"]

submit_runs(historical, task="thresholds")

submit_runs(df)
