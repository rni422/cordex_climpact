# ------------------------------------------------
# This wrapper script calls the 'create.thresholds.from.file' function from the modified climdex.pcic.ncdf package
# to create thresholds, using data and parameters provided by the user.
# ------------------------------------------------

library(climdex.pcic.ncdf)
# list of one to three input files. e.g. c("a.nc","b.nc","c.nc")
input.files=c("/g/data/w40/yl1269/climpact_indices/tasmin_AUS-44i_NCC-NorESM1-M_historical_r1i1p1_CSIRO-CCAM-2008_v1_day_19600101-20051231.mod.nc", "/g/data/w40/yl1269/climpact_indices/tasmax_AUS-44i_NCC-NorESM1-M_historical_r1i1p1_CSIRO-CCAM-2008_v1_day_19600101-20051231.mod.nc", "/g/data/w40/yl1269/climpact_indices/pr_AUS-44i_NCC-NorESM1-M_historical_r1i1p1_CSIRO-CCAM-2008_v1_day_19600101-20051231.nc")

#input.files="/g/data/w40/yl1269/climpact_indices/merged.nc"

# list of variable names according to above file(s)
vars=c(tmax="tasmax", tmin="tasmin", prec="pr")
#vars=c(prec="pr")

# output file name
output.file="/g/data/w40/yl1269/climpact_indices/threshold.nc"

# author data
author.data=list(institution="UNSW", institution_id="UNSW")

# reference period
base.range=c(1971,2000)

# number of cores to use (or FALSE)
cores=FALSE#24

# print messages?
verbose=TRUE

# Directory where Climpact is stored. Use full pathname. Leave as NULL if you are running this script from the Climpact directory (where this script was initially stored).
root.dir=NULL



######################################
# Do not modify without a good reason.

fclimdex.compatible=FALSE

create.thresholds.from.file(input.files,output.file,author.data,variable.name.map=vars,base.range=base.range,parallel=cores,verbose=verbose,fclimdex.compatible=fclimdex.compatible,root.dir=root.dir)
