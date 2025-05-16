#!/usr/bin/env bash
set -eu -o pipefail; shopt -s failglob

## Account settings
PROJECT="nn9560k"    # Where source code is located
ACCOUNT="nn9560k"    # Who pays for CPU hours
## See .../query_testlists --list machine
MACHINE=${MACHINE:-"betzy"}
## See .../query_testlists --list compiler
COMPILER=${COMPILER:-"intel"}

## What base tag version of NorESM are we testing
TAG=${TAG:-"noresm3_0_alpha03b"}
SRC_LABEL=${TAG}

## LABEL is the case name that will be used for the test
LABEL=${LABEL:-"3.0a03b"}

## Case label (overwrite)
LABEL="rest-NOIIAJRARYF6162OC_20250506.${LABEL}"
#LABEL="hybrid_fates-nocomp.${LABEL}"
# LABEL="hybrid_fatesnocomp.${LABEL}"
timestamp=$(date +"%Y%m%d")

## Compset long name
CSHORT=${CSHORT:-"n1850"}
CTIME=${CTIME:-"1850"}
CATM=${CATM:-"CAM70%LT%NORESM%CAMoslo"}
#CLND=${CLND:-"CLM60%FATES-NOCOMP"}
CLND=${CLND:-"CLM60%FATES"}
CICE=${CICE:-"CICE"}
COCE=${COCE:-"BLOM%HYB%ECO"}
CROF=${CROF:-"MOSART"}
CGLC=${CFLC:-"DGLC%NOEVOLVE"}
CWAV=${CWAV:-"SWAV"}
COMPSET=${COMPSET:-"${CTIME}_${CATM}_${CLND}_${CICE}_${COCE}_${CROF}_${CGLC}_${CWAV}_SESP"}

## Resolution
RES=${RES:-"ne30pg3_tn14"}
RES_LABEL=${RES_LABEL:-"ne30_tn14"}

## Component settings
## BLOM_TAG: This can be either a tag or a branch
##    tag: "<tag_name>"
##    branch: "develop/<branch_name>"
## BLOM_REPO: source for development version, either url to remote
##            repository or local path
BLOM_DEVELOP="no"
if [ "${BLOM_DEVELOP}" == "yes" ]; then
    #BLOM_LABEL=${BLOM_LABEL:-"output_switches"}
    #BLOM_LABEL=${BLOM_LABEL:-"master"}
    #BLOM_TAG=${BLOM_TAG:-"${BLOM_LABEL}"}           # Use this for tags
    #BLOM_TAG=${BLOM_TAG:-"develop/${BLOM_LABEL}"}   # Use this for branches
    BLOM_REPO=${BLOM_REPO:-"https://github.com/NorESMhub/BLOM"}
    #BLOM_REPO=${BLOM_REPO:-"https://github.com/TomasTorsvik/BLOM-TTfork"}
    #BLOM_REPO=${BLOM_REPO:-"https://github.com/jmaerz/BLOM"}
    SRC_LABEL="${SRC_LABEL}.BLOM-${BLOM_LABEL}"
fi

## Decide if you run model in debug mode
## - Debug flags enabled
RUN_DEBUG_MODE="no"

## Decied if we run with the --run-unsupported flag
## This should be used for compsets for which there are no
## science supported experiments available
RUN_UNSUPPORTED="yes"

## Decide whether or not to run manage_externals/checkout_externals
## While this is not perfect, it works if used consistently
## NB: If you check out the target tag from outside the script,
## be sure to run checkout_externals yourself, this script might
## miss it.
RUN_CHECKOUT="no"

## Source and case directories
REPO=${REPO:-"https://github.com/NorESMhub/NorESM"}
SRCROOT="/cluster/projects/${PROJECT}/${USER}/NorESM/src/${SRC_LABEL}"
CASEROOT="/cluster/projects/${PROJECT}/${USER}/NorESM/cases"
CASENAME="${CSHORT}.${RES_LABEL}.${LABEL}.${timestamp}"
CASEDIR="${CASEROOT}/${CASENAME}"
RUNDIR="/cluster/work/users/${USER}/noresm/${CASENAME}/run/"

## Include usermods?
USE_USERMODS="yes"
if [ "${USE_USERMODS}" == "yes" ]; then
    USERMODS="${SRCROOT}/cime_config/usermods_dirs/reduced_out_devsim/"
fi

# Copy this run script to CASEDIR
COPY_THIS_FILE="yes"
this_file="${PWD}/"; this_file+=$(basename "$0")
echo "Running ${this_file}"


##--- Function: Check for execution errors ---
perror() {
    ## Print an error message and exit if a non-zero error code is passed
    if [ $1 -ne 0 ]; then
        echo "ERROR (${1}): ${2}"
        exit $1
    fi
}

##--- Checkout model source code ---
## (make sure that clone exists, otherwise, clone REPO)
if [ ! -d "${SRCROOT}" ]; then
    git clone -o NorESM ${REPO} ${SRCROOT}
    perror $? "running 'git clone -o NorESM ${REPO} ${SRCROOT}'"
    RUN_CHECKOUT="yes"
fi

## Ensure correct source is checked out
cd ${SRCROOT}
if [ "${RUN_CHECKOUT}" != "no" ]; then
    if [ "$( git describe )" != "${TAG}" ]; then
        git checkout ${TAG}
        perror $? "running 'git checkout ${TAG}'"
    fi

    #--- Checkout TAG version ---
    ./bin/git-fleximod update
    perror $? "running './bin/git-fleximod update'"

    #--- Checkout BLOM development version ---
    if [ "${BLOM_DEVELOP}" == "yes" ]; then
        cd components/blom
        ## Remove old "develop" repo if present
        if git config remote.develop.url > /dev/null; then
            git remote rm develop
        fi
        git remote add develop ${BLOM_REPO}
        git fetch --all
        git fetch --tags --all
        git checkout --detach ${BLOM_TAG}
        git submodule update
        cd ${SRCROOT}
    fi
fi


##--- Create your case
cd ${SRCROOT}
if [ ! -d "${CASEDIR}" ]; then
    ## Only run create_newcase if the case does not exist
    echo "=== Creating new case ==="
    cn_args=" --case ${CASEDIR} --compset ${COMPSET} --res ${RES} --project ${ACCOUNT} --machine ${MACHINE} --compiler ${COMPILER}"
    if [ "${RUN_UNSUPPORTED}" == "yes" ]; then
        cn_args="${cn_args} --run-unsupported"
    fi
    if [ "${USE_USERMODS}" == "yes" ]; then
        cn_args="${cn_args} --user-mods-dir ${USERMODS}"
    fi
    ./cime/scripts/create_newcase ${cn_args}
    perror $? "running './cime/scripts/create_newcase ${cn_args}'"
fi


## Move to your case directory
cd ${CASEDIR}
perror $? "trying 'cd ${CASEDIR}'"
if [ "${COPY_THIS_FILE}" == "yes" ]; then
    cp ${this_file} ${CASEDIR}
    perror $? "running 'cp ${this_file} ${CASEDIR}'"
fi


## Copy noresm setup files
#NORESM_SETUP_DIR="/cluster/projects/nn9560k/tomast/NorESM/src/"
#cp -r ${NORESM_SETUP_DIR}/SourceMods/hybrid_wavparam_fatessp/* ./SourceMods/


## Any PE changes must go here
./xmlchange NTASKS=1920
./xmlchange NTASKS_OCN=256
./xmlchange ROOTPE=0
./xmlchange ROOTPE_OCN=1920

## Set up the case as configured so far
echo "=== Setup for new case ==="
./case.setup
perror $? "trying './case.setup'"

## Changes that affect the build go here
./xmlchange STOP_OPTION=nyears
./xmlchange STOP_N=4
./xmlchange REST_N=1
./xmlchange REST_OPTION=nyears
./xmlchange RESUBMIT=0
./xmlchange --subgroup case.run JOB_WALLCLOCK_TIME=48:00:00
./xmlchange --subgroup case.st_archive JOB_WALLCLOCK_TIME=03:00:00

## Changes to user_nl_* files goes here
cat <<EOF >> user_nl_cam
use_aerocom = .false.
interpolate_nlat   = 192
interpolate_nlon   = 288
interpolate_output = .true.
history_aerosol = .false.
zmconv_c0_lnd  =  0.0075D0
zmconv_c0_ocn  =  0.0300D0
zmconv_ke =  5.0E-6
zmconv_ke_lnd =  1.0E-5
clim_modal_aero_top_press =  1.D-4
micro_mg_dcs = 600.D-6
bndtvg = '/cluster/shared/noresm/inputdata/atm/cam/ggas/noaamisc.r8.nc'
EOF
perror $? "adding variables to user_nl_cam"


# cat <<EOF >> user_nl_blom
# ICFILE = "/cluster/shared/noresm/inputdata/restart/NOIIAJRAOC20TR_TL319_tn14_ppm_20240816/rest/1775-01-01-00000/NOIIAJRAOC20TR_TL319_tn14_ppm_20240816.blom.r.1775-01-01-00000.nc"
# EOF
# cat <<EOF >> user_nl_blom
# ICFILE = "/cluster/shared/noresm/inputdata/restart/NOIIAJRAOC_TL319_tn14_3.0a3_newmix_20250430/rest/0062-01-01-00000/NOIIAJRAOC_TL319_tn14_3.0a3_newmix_20250430.blom.r.0062-01-01-00000.nc"
# EOF
cat <<EOF >> user_nl_blom
ICFILE = "/cluster/shared/noresm/inputdata/restart/NOIIAJRARYF6162OC.TL319_tn14.3.0a03a.20250506/rest/1774-01-01-00000/NOIIAJRARYF6162OC.TL319_tn14.3.0a03a.20250506.blom.r.1774-01-01-00000.nc"
EOF
perror $? "adding variables to user_nl_blom"

# cat <<EOF >> user_nl_cice
# ice_ic = "/cluster/shared/noresm/inputdata/restart/NOIIAJRAOC20TR_TL319_tn14_ppm_20240816/rest/1775-01-01-00000/NOIIAJRAOC20TR_TL319_tn14_ppm_20240816.cice.r.1775-01-01-00000.nc"
# EOF
# cat <<EOF >> user_nl_cice
# ice_ic = "/cluster/shared/noresm/inputdata/restart/NOIIAJRAOC_TL319_tn14_3.0a3_newmix_20250430/rest/0062-01-01-00000/NOIIAJRAOC_TL319_tn14_3.0a3_newmix_20250430.cice.r.0062-01-01-00000.nc"
# EOF
cat <<EOF >> user_nl_cice
ice_ic = "/cluster/shared/noresm/inputdata/restart/NOIIAJRARYF6162OC.TL319_tn14.3.0a03a.20250506/rest/1774-01-01-00000/NOIIAJRARYF6162OC.TL319_tn14.3.0a03a.20250506.cice.r.1774-01-01-00000.nc"
EOF
perror $? "adding variables to user_nl_cice"

cat <<EOF >> user_nl_clm
use_fates_nocomp = .true.
use_fates_fixed_biogeog = .true.
use_fates_luh = .false.
fates_radiation_model = 'twostream'
finidat = '/cluster/work/users/kjetisaa/archive/i1850.FATES-NOCOMP-noLU_cal15_twosteam.ne30pg3_tn14.noresm3_0_alpha01.20250326/rest/0101-01-01-00000/i1850.FATES-NOCOMP-noLU_cal15_twosteam.ne30pg3_tn14.noresm3_0_alpha01.20250326.clm2.r.0101-01-01-00000.nc'
fates_history_dimlevel = 1,1
EOF
perror $? "adding variables to user_nl_clm"


## Build and submit case
echo "=== Build the case ==="
./preview_namelists
./case.build
perror $? "trying './case.build'"


echo "=== Submit the case ==="
./case.submit
perror $? "trying './case.submit'"
