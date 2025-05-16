#!/usr/bin/env bash
set -eu -o pipefail; shopt -s failglob

## Experiment settings
PROJECT="nn9560k"    # Where source code is located
ACCOUNT="nn9560k"    # Who pays for CPU hours
## See .../query_testlists --list machine
XML_MACHINE=${XML_MACHINE:-"betzy"}
## See .../query_testlists --list compiler
## Default: if not set, use all available compiler options
## options: "intel", "gnu"
XML_COMPILER=${XML_COMPILER:-""}

## What base version of NorESM are we testing
## options: "noresm2.0", "noresm2.1", "noresm2.3", "noresm2.5", "noresm3.0"
NORESM_VERSION=${NORESM_VERSION:-"noresm3.0"}

## See $SRCROOT/cime/scripts/query_testlists for all available options.
## NorESM categories have the suffix "_noresm"
## options: allactive, blom, cam, clm, hamocc
TEST_CATEGORY="blom"

## Decide if you will generate baselines
## options: "yes", "no"
GENERATE_BASELINES="no"
# LABEL="feature-noresm3_0_alpha03b"   # optional, default LABEL is generated from TAG
# BASELINE_NAME="noresm3_0_alpha03b"   # optional, default is LABEL

## Baseline comparison with release version should mainly refrence root folder
## options: "yes", "no"
MAKE_RELEASE_TEST="no"

## Decide if you will compare against baselines
## options: "yes", "no"
COMPARE_BASELINES="no"

## Will we compare with a release version baseline?
## options: "yes", "no"
COMPARE_WITH_RELEASE="no"

## Allow baseline overwrite
## options: "yes", "no"
OVERWRITE_BASELINES="yes"

## TEST_TYPE :  "c" : XML_CATEGORY
##              "f" : FILE
##              "s" : single test
TEST_TYPE="s"

## Decide if you run model in debug mode
## - Debug flags enabled
## options: "yes", "no"
RUN_DEBUG_MODE="no"

## Decide if you want to retry failed tests
##   >0 implies --wait, i.e. the script will wait until
##   submitted jobs have completed
RETRY=0

## Decied if we run with the --run-unsupported flag
## This should be used for compsets for which there are no
## science supported experiments available
## options: "yes", "no"
RUN_UNSUPPORTED="yes"

## Decide whether or not to run manage_externals/checkout_externals
## While this is not perfect, it works if used consistently
## NB: If you check out the target tag from outside the script,
## be sure to run checkout_externals yourself, this script might
## miss it.
## options: "yes", "no"
RUN_CHECKOUT="no"

##-----------------------

##--- Test setup ---
##
##----- Test category
case ${TEST_CATEGORY} in
    "allactive")
        XML_CATEGORY=${XML_CATEGORY:-"prealpha_noresm"}
        BASELINE_ROOT=${BASELINE_ROOT:-"/cluster/shared/noresm/noresm_baselines/noresm_develop"}
        COMPARE_ROOT=${COMPARE_ROOT:-"/cluster/shared/noresm/noresm_baselines/noresm_develop"}
        #TESTFILE=${TESTFILE:-"${PWD}/ALL_testfile.txt"}
        #TESTFILE=${TESTFILE:-"${PWD}/ALL_N2.3_testfile.txt"}
        ;;
    "blom")
        XML_CATEGORY=${XML_CATEGORY:-"aux_blom_noresm"}
        BASELINE_ROOT=${BASELINE_ROOT:-"/cluster/shared/noresm/noresm_baselines/blom_develop"}
        COMPARE_ROOT=${COMPARE_ROOT:-"/cluster/shared/noresm/noresm_baselines/blom_develop"}
        #TESTFILE=${TESTFILE:-"${PWD}/BLOM_testfile.txt"}
        ;;
    "cam")
        XML_CATEGORY=${XML_CATEGORY:-"aux_cam_noresm"}
        BASELINE_ROOT=${BASELINE_ROOT:-"/cluster/shared/noresm/noresm_baselines/cam_develop"}
        COMPARE_ROOT=${COMPARE_ROOT:-"/cluster/shared/noresm/noresm_baselines/cam_develop"}
        ;;
    "clm")
        XML_CATEGORY=${XML_CATEGORY:-"aux_clm_noresm"}
        BASELINE_ROOT=${BASELINE_ROOT:-"/cluster/shared/noresm/noresm_baselines/ctsm_develop"}
        COMPARE_ROOT=${COMPARE_ROOT:-"/cluster/shared/noresm/noresm_baselines/ctsm_develop"}
        ;;
    "hamocc")
        XML_CATEGORY=${XML_CATEGORY:-"aux_hamocc_noresm"}
        BASELINE_ROOT=${BASELINE_ROOT:-"/cluster/shared/noresm/noresm_baselines/blom_develop"}
        COMPARE_ROOT=${COMPARE_ROOT:-"/cluster/shared/noresm/noresm_baselines/blom_develop"}
        ;;
    *)
        echo "ERROR: Unknown TEST_CATEGORY ${TEST_CATEGORY}"
        exit
        ;;
esac
if [ "${MAKE_RELEASE_TEST}" == "yes" ]; then
    BASELINE_ROOT="/cluster/shared/noresm/noresm_baselines"
fi
if [ "${COMPARE_WITH_RELEASE}" == "yes" ]; then
    COMPARE_ROOT="/cluster/shared/noresm/noresm_baselines"
fi
##
##----- Test type
case ${TEST_TYPE} in
    "c")
        TEST_TYPE_STR=${TEST_TYPE_STR:-"--xml-category ${XML_CATEGORY}"}
        ;;
    "f")
        TEST_TYPE_STR=${TEST_TYPE_STR:-"--testfile ${TESTFILE}"}
        ;;
    "s")
        ### single test from "blom" category
        TESTNAME=${TESTNAME:-"SMS_D_Ld1.T62_tn14.NOINYOC.betzy_intel"}
        #TESTNAME=${TESTNAME:-"SMS_D_Ld1.T62_tn14.NOINYOC.betzy_intel"}
        #TESTNAME=${TESTNAME:-"PET_Ld3.T62_tn14.NOINY.betzy_intel"}
        #TESTNAME=${TESTNAME:-"PEM_Ld3.T62_tn14.NOINY.betzy_intel"}
        #TESTNAME=${TESTNAME:-"PET_Ld5.f19_tn14.N1850.betzy_intel"}
        ### single test from "cam" category
        #TESTNAME=${TESTNAME:-"ERP_D_Ln9.f19_f19_mtn14.QPC6.betzy_intel.cam-outfrq9s"}
        ### single test from "allactive" category
        #TESTNAME=${TESTNAME:-"SMS_D_Ld1.ne16pg3_tn14.N1850fates-sp.betzy_gnu.allactive-defaultio"}
        #TESTNAME=${TESTNAME:-"SMS_D_Ld1.ne30pg3_tn14.N1850fates-sp.betzy_gnu.allactive-defaultio"}
        #TESTNAME=${TESTNAME:-"SMS_D_Ld1.ne30pg3_tn14.N1850fates-nocomp.betzy_intel.allactive-defaultio"}
        #TESTNAME=${TESTNAME:-"ERI.ne30pg3_tn14.N1850.betzy_intel.allactive-defaultio"}
        #TESTNAME=${TESTNAME:-"ERS_Ld5.f19_tn14.NHIST.betzy_intel.allactive-defaultio"}
        #TESTNAME=${TESTNAME:-"ERS_D_Ld4.ne30pg3_tn14.N2N1850.betzy_intel.allactive-defaultio"}
        #TESTNAME=${TESTNAME:-"PFS.f09_tn14.NHISTfrc2.betzy_intel.allactive-defaultio"}
        ###
        TEST_TYPE_STR=${TEST_TYPE_STR:-"${TESTNAME}"}
        ;;
    *)
        echo "ERROR: Unknown TEST_TYPE ${TEST_TYPE}"
        exit
        ;;
esac
##
##----- Model settings
## TAG should either be a release name, tag name or branch name
case ${NORESM_VERSION} in
    "noresm2.0")
        TAG=${TAG:-"release-noresm2.0.9"}
        XML_COMPILER="intel"
        if [ "${COMPARE_WITH_RELEASE}" == "yes" ]; then
            COMPARE_NAME=${COMPARE_NAME:-"release-noresm2.0.8"}
        else
            case ${TEST_CATEGORY} in
                "blom" | "hamocc")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2.0.7_v1.4.1"}
                    ;;
                *)
                    echo "No baseline comparison case selected."
                    ;;
            esac
        fi
        ## Are we using git-fleximod?
        USE_GITFLEXIMOD="no"
        ;;
    "noresm2.1")
        TAG=${TAG:-"release-noresm2.1.3"}
        XML_COMPILER="intel"
        if [ "${COMPARE_WITH_RELEASE}" == "yes" ]; then
            COMPARE_NAME=${COMPARE_NAME:-"release-noresm2.1.2"}
        else
            case ${TEST_CATEGORY} in
                "blom" | "hamocc")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2.1.1_dev1.5.1.3"}
                    ;;
                *)
                    echo "No baseline comparison case selected."
                    ;;
            esac
        fi
        ## Are we using git-fleximod?
        USE_GITFLEXIMOD="no"
        ;;
    "noresm2.3")
        TAG=${TAG:-"noresm2_3_alpha03"}
        XML_COMPILER="intel"
        if [ "${COMPARE_WITH_RELEASE}" == "yes" ]; then
            COMPARE_NAME=${COMPARE_NAME:-"release-noresm2.1.4"}
        else
            case ${TEST_CATEGORY} in
                "allactive")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2_3_alpha03_dev1.7.1.4"}
                    ;;
                "blom" | "hamocc")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2_3_alpha02"}
                    ;;
                "cam")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2_3_alpha02"}
                    ;;
                *)
                    echo "No baseline comparison case selected."
                    ;;
            esac
        fi
        ## Are we using git-fleximod?
        USE_GITFLEXIMOD="no"
        ;;
    "noresm2.5")
        TAG=${TAG:-"noresm2_5_alpha09a"}
        if [ "${COMPARE_WITH_RELEASE}" == "yes" ]; then
            COMPARE_NAME=${COMPARE_NAME:-"release-noresm2.1.4"}
        else
            case ${TEST_CATEGORY} in
                "allactive")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2_5_alpha09a_dev1.7.1.4"}
                    ;;
                "blom" | "hamocc")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm2_5_alpha09a_dev1.7.1.4"}
                    ;;
                #"cam")
                #    COMPARE_NAME=${COMPARE_NAME:-"noresm2_5_alpha08d"}
                #    ;;
                *)
                    echo "No baseline comparison case selected."
                    ;;
            esac
        fi
        ## Are we using git-fleximod?
        USE_GITFLEXIMOD="yes"
        ;;
    "noresm3.0")
        TAG=${TAG:-"noresm3_0_alpha03b"}
        if [ "${COMPARE_WITH_RELEASE}" == "yes" ]; then
            COMPARE_NAME=${COMPARE_NAME:-"release-noresm2.1.4"}
        else
            case ${TEST_CATEGORY} in
                "allactive")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm3_0_alpha03b"}
                    ;;
                "blom" | "hamocc")
                    COMPARE_NAME=${COMPARE_NAME:-"noresm3_0_alpha03b"}
                    ;;
                #"cam")
                #    COMPARE_NAME=${COMPARE_NAME:-"noresm2_5_alpha08d"}
                #    ;;
                *)
                    echo "No baseline comparison case selected."
                    ;;
            esac
        fi
        ## Are we using git-fleximod?
        USE_GITFLEXIMOD="yes"
        ;;
    *)
        echo "ERROR: Unknown NORESM_VERSION ${NORESM_VERSION}"
        exit
        ;;
esac

## LABEL is the case name that will be used for the test
LABEL=${LABEL:-${TAG}}

## Component settings
## BLOM_TAG: This can be either a tag or a branch
##    tag: "<tag_name>"
##    branch: "develop/<branch_name>"
## BLOM_REPO: source for development version, either url to remote
##            repository or local path
BLOM_DEVELOP="yes"
if [ "${BLOM_DEVELOP}" == "yes" ]; then
    BLOM_IS_TAG="no"
    if [ "${BLOM_IS_TAG}" == "yes" ]; then
        #BLOM_LABEL=${BLOM_LABEL:-"v1.4.1"}
        #BLOM_LABEL=${BLOM_LABEL:-"v1.6.8"}
        #BLOM_LABEL=${BLOM_LABEL:-"dev1.7.1.4"}
        #BLOM_LABEL=${BLOM_LABEL:-"v1.7.2"}
        BLOM_LABEL=${BLOM_LABEL:-"dev1.9.0.13"}
        BLOM_TAG=${BLOM_TAG:-"${BLOM_LABEL}"}           # Use this for tags
        BLOM_REPO=${BLOM_REPO:-"https://github.com/NorESMhub/BLOM"}
    else
        BLOM_LABEL=${BLOM_LABEL:-"master"}
        #BLOM_LABEL=${BLOM_LABEL:-"seaice_enhanced_gas_fluxes_noresm2.0"}
        #BLOM_LABEL=${BLOM_LABEL:-"hamocc_reset_dustflx"}
        #BLOM_LABEL=${BLOM_LABEL:-"main_add_gnu_tests"}
        #BLOM_LABEL=${BLOM_LABEL:-"rpointers-update-blom"}
        #BLOM_LABEL=${BLOM_LABEL:-"feature-whole_array_initialization"}
        BLOM_TAG=${BLOM_TAG:-"develop/${BLOM_LABEL}"}   # Use this for branches
        BLOM_REPO=${BLOM_REPO:-"https://github.com/TomasTorsvik/BLOM-TTfork"}
        #BLOM_REPO=${BLOM_REPO:-"https://github.com/matsbn/BLOM"}
        #BLOM_REPO=${BLOM_REPO:-"https://github.com/mvdebolskiy/BLOM"}
    fi
    #BLOM_UNIT="cgs"
    #SET_HAMOCC_ICE_LEADFRAC=${SET_HAMOCC_ICE_LEADFRAC:-"0.02"}
    LABEL="${LABEL}_${BLOM_LABEL}"
fi

## Source and case directories
REPO=${REPO:-"https://github.com/NorESMhub/NorESM"}
SRCROOT="/cluster/projects/${PROJECT}/${USER}/NorESM/src/${LABEL}"


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

    if [ "${USE_GITFLEXIMOD}" == "yes" ]; then
        ./bin/git-fleximod update
        perror $? "running './bin/git-fleximod update'"
    else
        ./manage_externals/checkout_externals
        perror $? "running './manage_externals/checkout_externals'"
    fi

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
        if [ "${USE_GITFLEXIMOD}" == "yes" ]; then
            git submodule update
        fi
        cd ${SRCROOT}
    fi
fi


##--- Create test case ---
cn_args=" --project ${ACCOUNT} --xml-machine ${XML_MACHINE}"
cn_args="${cn_args} --output-root ${USERWORK}/noresm/${LABEL} ${TEST_TYPE_STR}"
cn_args="${cn_args} --retry ${RETRY}"

# check if compiler is set
if [ ! -z "${XML_COMPILER}" ]; then
    cn_args="${cn_args} --xml-compiler ${XML_COMPILER}"
fi

if [ "${GENERATE_BASELINES}" == "yes" ]; then
    BASELINE_NAME=${BASELINE_NAME:-${LABEL}}      # Do not overwrite if already set
    cn_args="${cn_args} --baseline-root ${BASELINE_ROOT} --generate ${BASELINE_NAME}"
fi

if [ "${COMPARE_BASELINES}" == "yes" ]; then
    cn_args="${cn_args} --baseline-root ${COMPARE_ROOT} --compare ${COMPARE_NAME}"
fi

if [ "${OVERWRITE_BASELINES}" == "yes" ]; then
    cn_args="${cn_args} --allow-baseline-overwrite"
fi


./cime/scripts/create_test ${cn_args}
perror $? "running './cime/scripts/create_test ${cn_args}'"

