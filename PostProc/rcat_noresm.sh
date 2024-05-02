#!/bin/bash

### Tool to concatenate and compress NorESM day-files to month- or year-files
##
## Currently able to handle output files from 'atm', 'ice', 'lnd',
##    'ocn', and 'rof' components.

# Modified by Steve Goldhaber Met, 2023
# Original version from Tyge Løvset NORCE, last modification 2022
# use on e.g.:
# /projects/NS9560K/noresm/cases/N1850frc2_f09_tn14_20191113

## Update the script version here to indicate changes
## Use semantic versioning (https://semver.org/).
VERSION="0.0.7"

# Store some pathnames, find tools needed by this script
tool=$(basename $0)
tooldir=$(dirname $(realpath $0))
bindir="/projects/NS9560K/local/bin"
cprnc=${tooldir}/cprnc
xxhsum=${tooldir}/xxhsum
if [ ! -x "${cprnc}" ]; then
    cprnc="${bindir}/cprnc"
fi
if [ ! -x "${cprnc}" ]; then
    echo  "ERROR: No cprnc tool found, should be installed at ${bindir}"
    exit 1
fi
if [ ! -x "${xxhsum}" ]; then
    xxhsum="${bindir}/xxhsum"
fi
if [ ! -x "${xxhsum}" ]; then
    echo  "ERROR: No xxhsum tool found, should be installed at ${bindir}"
    exit 1
fi

## Need to set the locale to be compatible with ncks output
LC_NUMERIC="en_US.UTF-8"

## Variables for optional arguments
COMPARE="None" ## alternatives are 'Spot' or 'Full'
COMPONENTS=()
COMPRESS=2
DELETE=0
DRYRUN="no"
ERRCODE=0
ERRMSG=""
KEEP_MONTHLY="no" # Do not concatenate monthly files into yearly files
MOVEDIR="/scratch/${USER}/SOURCE_FILES_TO_BE_DELETED"
MOVE=0
MERGETYPE="yearly" # or "monthly" or "mergeall" or "compressonly"
declare -i NTHREADS=4
POSITIONAL=()
PROGREPINT=1000
USEICEFILENAMES="yes"
UNITTESTMODE="no"  # Used for unit testing, skip input checking, error checks, and runs
declare -i VERBOSE=0   # Use --verbose to get more output

## Error codes. 0 and 1 are reserved for true and false Bash primitives
SUCCESS=02             # Routine ran without error
ERR_BADARG=03          # Bad command line argument
ERR_BADARG_GT=04       # Bad argument(s) to greater_than
ERR_BADARG_LT=05       # Bad argument(s) to less_than
ERR_BADYEAR=06         # Bad year found in history file
ERR_BAD_COMPTYPE=07    # Unknown component type
ERR_BAD_DATESTR=08     # Bad date string from ncks
ERR_BAD_MERGETYPE=09   # Bad (unknown) merge type
ERR_BAD_TIME=10        # Error extracting time using ncks
ERR_BAD_YEAR0=11       # Error extracting year0 from calendar
ERR_CHECKSUM=12        # Error creating a checksum
ERR_COMPARE=13         # Error comparing original and compressed data
ERR_COPY=14            # Error copying file
ERR_CPRNC=15           # Error running cprnc
ERR_EXTRACT=16         # Error extracting frame using ncks
ERR_INTERNAL=17        # Internal error (should not happen)
ERR_INTERRUPT=18       # User or system interrupt
ERR_MISSING_FILE=19    # File does not exist
ERR_MULTMONTHS=20      # Multiple months found in history file
ERR_MULTYEARS=21       # Multiple years found in history file
ERR_NCKS_MDATA=22      # Error extracting file metadata with ncks
ERR_NCRCAT=23          # Error running ncrcat
ERR_NOACCESS=24
ERR_NOCOMPRESS=25      # No files to compress
ERR_UNSUPPORT_CAL=26   # Unsupported NetCDF calendar type
ERR_UNSUPPORT_MERGE=27 # Unsupported merge type for file
ERR_UNSUPPORT_TIME=28  # Unsupported NetCDF time units

## Keep track of failures
declare -A fail_count=() # Count of job failures
declare -A job_status=() # Status = created, in compress, compressed, in check, in copy, copied, pass, fail, ERROR
declare -A error_reports=()
## Have a global logfile that can be of use even in the case of an error exit
declare logfilename=""
## Have a lock file that is created during errors from threaded regions
declare err_lockfilename=""
## Use a single timestamp for the logfile and xxhsum files
declare JOBLID
## Only report the job status once
declare job_report_done="no"
## Path to NCO tools
declare ncrcat
declare ncks
## Top directory of output
declare outpath

help() {
    echo -e "${tool}, version ${VERSION}\n"
    echo "Tool to compress and convert NorESM day-files to month- or year-files"
    echo "Usage:"
    echo "  ${tool} [OPTIONS] <archive case path> <output path>"
    echo "       --comp              component (default 'ice:cice')"
    echo "   -y  --year  --yearly    merge per year (default)"
    echo "   -m  --month --monthly   merge per month"
    echo "   -a  --merge-all         merge all files"
    echo "       --compress-only     compress files but do not merge"
    echo "       --keep-monthly      do not concatenate monthly files into"
    echo "                           yearly files"
    echo "   -c  --compress N        compression strength 1-9 (default 2)"
    echo "   -t  --threads N         parallel run (default 4)"
    echo "       --compare <option>  Options are \"Spot\" (spot check),"
    echo "                           \"Full\", or \"None\" (default)"
    echo "       --verbose           Output and log more information about"
    echo "                           work in progress"
    echo "       --move              move source files to scratch after merge"
    echo "       --delete            move and delete source files when done"
    echo "       --dryrun            display the files to be merged but do"
    echo "                           not perform any actions"
    echo "       --check-ice-files   retrieve dates from the ice history files"
    echo "                           (slow) instead of trusting the filenames"
    echo "   -v  --version           print the current script version and exit"
    echo "   -h  --help              print this message and exit"
    echo ""
    echo "To compress multiple components, use --comp <comp> once per component"
    echo "--compare checks the fidelity of the merged file against a selection"
    echo "          (spot) or every (full) source file. It does this by"
    echo "          comparing the frame(s) of a source file with the"
    echo "          corresponding frames of the merged file."
    echo "          Any differences are reported and cause an error exit."
    echo "-y, --year, --yearly, -m, --month, --monthly, -a, --merge-all,"
    echo "    and --compress-only control the concatenation of files."
    echo "    Choose one option (if multiple options are selected, only"
    echo "    the last one entered is used)"
    echo "Note that --keep-monthly only has an effect if the merge type is"
    echo "     'year' or 'yearly'"
    echo "The <output path> argument has the same form as <archive case path>;"
    echo "    The compressed files are stored in a directory under"
    echo "    <output path> (e.g., '<output path>/ice/hist' or"
    echo "    '<output path>/rest/1850-05-01-00000')."
    echo "Using --verbose more than once increases the amount of output"
    if [ $# -gt 0 ]; then
        exit $1
    else
        exit
    fi
}

log() {
    ## Echo a message ($@) to the terminal with a copy to the logfile
    echo -e "${@}" | tee -a ${logfilename}
}

qlog() {
    ## Write a message ($@) to the logfile
    if [ -n "${logfilename}" ]; then
        echo -e "${@}" >> ${logfilename}
    fi
}

add_error() {
    # Log an error and set the corresponding job status
    # $1 is the error code to log
    # $2 is the job key status to set
    # $3 is the error message
    local emsg=""
    ERRCODE=${!1}
    if [ "${1}" == "ERR_INTERNAL" ]; then
        emsg="INTERNAL ERROR ${ERRCODE}: ${3}"
    else
        emsg="ERROR ${ERRCODE}: ${3}"
    fi
    if [ -n "${2}" ]; then
        job_status[${2}]="${emsg}"
        fail_count[${2}]=$((fail_count[${2}] + 1))
        error_reports[${2}]=${ERRCODE}
    fi
    log "${emsg}"
    if [ -n "${ERRMSG}" ]; then
        ERRMSG="${ERRMSG}\n${emsg}"
    else
        ERRMSG="${emsg}"
    fi
    return ${ERRCODE}
}

add_fatal_error() {
    # Log a fatal error and set the corresponding job status
    # $1 is the error code to log
    # $2 is the job key status to set
    # $3 is the error message
    local res
    add_error ${1} "${2}" "${3}"
    res=$?
    if [ -n "${err_lockfilename}" ]; then
        echo -e "ERROR ${1}: ${3}" >> ${err_lockfilename}
    fi
    return ${res}
}

errlog() {
    ## Write an error message ($@)
    ## Error messages is echoed to the screen, the log file and the error log
    echo "${@}" | tee -a ${logfilename}
    if [ -n "${err_lockfilename}" ]; then
        echo -e "${@}" >> ${err_lockfilename}
    fi
}

fatal_error() {
    ## Return true if a fatal error condition exists, false otherwise
    if [ -n "${err_lockfilename}" -a -f "${err_lockfilename}" ]; then
        true
    else
        false
    fi
}

while [ $# -gt 0 ]; do
    key="${1}"
    case ${key} in
        --check-ice-files)
            USEICEFILENAMES="no"
            ;;
        --comp)
            if [ $# -lt 2 ]; then
                echo "--comp requires a component name argument"
                help
            fi
            COMPONENTS+=($2)
            shift
            ;;
        --compress-only)
            MERGETYPE="compressonly"
            ;;
        -a|--merge-all)
            MERGETYPE="mergeall"
            ;;
        -m|--month|--monthly)
            MERGETYPE="monthly"
            ;;
        -y|--year|--yearly)
            MERGETYPE="yearly"
            ;;
        --keep-monthly)
            KEEP_MONTHLY="yes"
            ;;
        -t|--threads)
            if [ $# -lt 2 ]; then
                echo "${key} requires a number of threads"
                help
            fi
            NTHREADS=$2
            shift
            ;;
        -c|--compress)
            if [ $# -lt 2 ]; then
                echo "${key} requires a compression level (number)"
                help
            fi
            COMPRESS=$2
            shift
            ;;
        --compare)
            if [ $# -lt 2 ]; then
                echo "${key} requires a comparison type"
                help
            fi
            if [ "${2,,}" == "full" ]; then
                COMPARE="Full"
            elif [ "${2,,}" == "spot" ]; then
                COMPARE="Spot"
            elif [ "${2,,}" == "none" ]; then
                COMPARE="None"
            else
                echo "Unknown option to --compare, '${2}'"
                help 1
            fi
            shift
            ;;
        --delete)
            MOVE=1
            DELETE=1
            ;;
        --move)
            MOVE=1
            ;;
        --no-check-ice-files)
            USEICEFILENAMES="yes"
            ;;
        --no-keep-monthly)
            KEEP_MONTHLY="yes"
            ;;
        --no-delete)
            DELETE=0
            ;;
        --no-move)
            DELETE=0
            MOVE=0
            ;;
        -h|--help)
            help
            ;;
        --verbose)
            VERBOSE=$((VERBOSE + 1))
            PROGREPINT=$((PROGREPINT / 10))
            if [ ${PROGREPINT} -lt 1 ]; then
                PROGREPINT=1
            fi
            ;;
        -v|--version)
            echo "${tool} version ${VERSION}"
            exit 0
            ;;
        --dryrun)
            DRYRUN="yes"
            ;;
        --unit-test-mode)
            ## Note, this is not documented (not a user-level switch)
            UNITTESTMODE="yes"
            ;;
        -*) # unknown
            echo "ERROR: Unknown argument, '${1}'"
            help 1
            ;;
        *) # positional arg
            POSITIONAL+=("$1")
            ;;
    esac
    shift
done
set -- "${POSITIONAL[@]}" # restore positional parameters

## Check correct number of positional parameters
if [ $# -ne 2 -a "${UNITTESTMODE}" == "no" ]; then
  help
fi

if [ "${UNITTESTMODE}" == "no" ]; then
    module load NCO/5.0.3-intel-2021b
    ulimit -s unlimited
fi

JOBLID=$(date +'%Y%m%d_%H%M%S')
if [ "${UNITTESTMODE}" == "no" ]; then
    ## We have a bit of chicken and egg here.
    ## We can't log until we have a <logfilename> but
    ## We can't create a <logfilename> until we have a
    ## (possibly newly created) output directory.
    logmsgs=""
    ncrcat=$(which ncrcat)
    ncks=$(which ncks)
    # The second positional argument is a location for output and logging
    if [ ! -d "${2}" ]; then
        logmsgs="Creating <output path>, '${2}'"
        mkdir -p ${2}
    fi
    outpath=$(realpath "${2}")
    if [ -f "${outpath}" -o ! -w "${outpath}" ]; then
        add_fatal_error ERR_BADARG ""                                         \
                        "Output path, \"${outpath}\", is not a writeable directory"
        exit $?
    fi
    logfilename="${outpath}/${tool}.${JOBLID}.log"
    err_lockfilename="${outpath}/${tool}.${JOBLID}.error"

    if [ -f "${logfilename}" ]; then
        rm -f ${logfilename}
    fi
    touch ${logfilename}
    if [ -n "${logmsgs}" ]; then
        log "${logmsgs}"
    fi

    # The first positional argument is the path to an existing case
    if [ ! -d "${1}" ]; then
        add_fatal_error ERR_BADARG ""                                         \
                        "<archive case path>, '${1}', does not exist"
        exit $?
    fi
    casepath=$(realpath "${1}")
    casename=$(basename ${casepath})
fi

if [ "${UNITTESTMODE}" == "no" ]; then
    touch ${logfilename}
    log "===================="
    log "${tool}"
    log "===================="
    log "NorESM Case: $casepath"
    log "Output Dir: ${outpath}"
    log "Compressing components: ${COMPONENTS[@]}"
    if [ ${MOVE} -eq 1 ]; then
      log "Move/Delete Dir: '${MOVEDIR}'"
    else
        log "Not moving any files"
    fi
    if [ "${MERGETYPE}" == "monthly" ]; then
        log "Merge files per month"
    elif [ "${MERGETYPE}" == "yearly" ]; then
        log "Merge files per year"
    elif [ "${MERGETYPE}" == "mergeall" ]; then
        log "Merge all files into one sequence"
    elif [ "${MERGETYPE}" == "compressonly" ]; then
        log "Do not merge files, compress only"
    else
        add_fatal_error ERR_BADARG ""                                         \
                        "Undefined merge type, '${MERGETYPE}'."
        exit $?
    fi
    if [ "${KEEP_MONTHLY}" == "yes" ]; then
        if [ "${MERGETYPE}" != "yearly" ]; then
            KEEP_MONTHLY="no"
            log "Ignoring --keep-monthly option since merge type is not 'yearly'"
        else
            log "Not merging monthly files into yearly files"
        fi
    fi
    log "Compression: ${COMPRESS}"
    log "Threads: ${NTHREADS}"
    log "Message Verbosity: Level ${VERBOSE}"
    log "===================="
    if [ ${VERBOSE} -ge 1 ]; then
        log "cprnc = ${cprnc}"
        log "xxhsum = ${xxhsum}"
        log "ncrcat = ${ncrcat}"
        log "ncks = ${ncks}"
        log "===================="
    fi
fi

##set -x

report_job_status() {
    ## Given a status code ($1) and the number of created jobs ($2),
    ##    check for errors.
    ## If any errors are found, report on the status of each job and
    ##    number of errors it encountered.
    local hfile
    local res=${1}
    local -i job_num=${2}
    local -i nfails
    local -i tjobs            # Total number of jobs
    local -i nerrs=${#error_reports[@]}
    if [ "${job_report_done}" != "no" ]; then
        return
    fi
    # Report on jobs run and any errors
    if ! fatal_error; then
        # Job number is not likely to match in the case of an error
        tjobs=${#job_status[@]}
        if [ ${tjobs} -ne ${job_num} ]; then
            ERRMSG="Internal error, job mismatch (${tjobs} != ${job_num})"
            errlog "ERROR ${ERRMSG}"
        fi
    fi
    nfails=$(echo "${fail_count[@]}" | tr ' ' '+' | bc)
    if fatal_error; then
        log "Job encountered fatal errors"
    elif [ ${nfails} -gt 0 ]; then
        for hfile in ${!job_status[@]}; do
            log "Job status for '${hfile}': ${job_status[${hfile}]}"
            if [ ${fail_count[${hfile}]} -gt 0 ]; then
                log "Output file: ${job_num} had ${fail_count[${job_num}]} FAILures"
            fi
        done
    elif [ ${nerrs} -eq 0 -a ${res} -eq 0 -a "${UNITTESTMODE}" == "no" ]; then
        log ${logile} "All tests PASSed"
    elif [ ${nerrs} -gt 0 ]; then
        log "Internal errors or errors running tools reported"
        for hfile in ${!error_reports[@]}; do
            log "${error_reports[${hfile}]}"
        done
    fi
    job_report_done="yes"
}

report_progress() {
    ## Update a progress bar, only outputs if stdout is a terminal
    ## $1 is the current counter
    ## $2 is the total work count
    ## $3 is the update interval
    ## $4 is a "pre" update message
    ## $5 is a "post" update message
    local -i count=${1}
    local -i tcnt=${2}
    local -i rep_int=${3}
    local premsg=${4}
    local postmsg=${5}
    if [ ${count} -eq ${tcnt} ]; then
        # Log completed progress to log file
        if [ -t 1 ]; then
            echo -e "\e[1A\e[K"
        fi
        log "${premsg}${count} / ${tcnt}${postmsg}"
    elif [ -t 1 ]; then
        # Progess bar only to terminal, not logged
        if [ $((${count} % rep_int)) -eq 0 -a ${count} -gt 0 ]; then
            echo -e "\e[1A\e[K${premsg}${count} / ${tcnt}${postmsg}"
        fi
    fi
}

kill_zombie_jobs() {
    ## Kill any jobs with a status listed as $1 (default Done)
    local item       # Part of a job status line
    local jobnum=""  # Job number
    local jobstat="" # Job status
    local killstat="Done"
    if [ $# -gt 0 ]; then
        killstat="${1}"
    fi
    for item in $(jobs); do
        if [ -n "${jobnum}" ]; then
            jobstat="${item}"
        fi
        if [[ "${item}" =~ \[([0-9]*)\] ]]; then
            jobnum="${BASH_REMATCH[1]}"
        fi
        if [ -n "${jobnum}" -a -n "${jobstat}" ]; then
            if [ "${jobstat}" == "${killstat}" ]; then
                echo "Killing ${jobstat} job, ${jobnum}"
                kill -9 "%${jobnum}" 2>&1 > /dev/null
            fi
            jobnum=""
            jobstat=""
        fi
    done
}

__cleanup() {
    # Cleanup on any error condition
    local res=$?
    if [ -n "${ERRMSG}" ]; then
        log ""
        log -e "ERROR: ${ERRMSG}"
    fi
    log ""
    if [ ${res} -ne 0 ]; then
        log "Exit code ${res} signaled"
        log "${tool} canceled: cleaning up .tmp files..."
        if [ -z "${ERRMSG}" ]; then
            log "The tool can be restarted and should continue conversion."
        fi
    fi
    if [ -n "$(ls ${outpath}/*.tmp 2> /dev/null)" ]; then
        rm -r ${outpath}/*.tmp
    fi
    rm -f ${err_lockfilename}
    report_job_status ${res} ${#job_status[@]}
    if [ ${ERRCODE} -ne 0 ]; then
        exit ${ERRCODE}
    else
        exit ${res}
    fi
}

__interrupt() {
    ## Special cleanup catch for when the user hits ^C
    ERRMSG="Job interrupted by user"
    exit ${ERR_INTERRUPT}
}

trap __cleanup EXIT
trap __interrupt SIGINT

num_jobs() {
    # Return the number of child processes
    local bash_pid=$$
    local children=$(ps -eo ppid | grep -w $bash_pid)
    echo "${children}"
}

get_file_set_name() {
    # Given a filename ($1), return the instance string and history file number.
    # For a multi-instance run, this will look like xxx_0001.h1 or xxx_0002.h3, etc.
    # For a single instance run, the return val will look like xxx.h1 or xxx.h2, etc.
    # In both cases, xxx will be a model name such as cam or clm.
    echo "$(echo ${1} | cut -d'.' -f2-3)"
}

get_file_set_names() {
    # Given an array of files ($1), return the set of instance strings and history file numbers.
    # For a multi-instance run, these will look like xxx_0001.h1, xxx_0002.h1. etc.
    # For a single instance run, the entries will look like xxx.h1, xxx.h2, etc.
    # In both cases, xxx will be a model name such as cam or clm.
    local istrs=($@)
    local set_names
    set_names=($(echo ${istrs[@]} | tr ' ' '\n' | cut -d'.' -f2-3 | sort | uniq))
    echo "$(echo ${set_names[@]} | sed -e 's/ /:/g')"
}

convert_time_to_date() {
    # Given a time ($1) and a base year ($2), return the date string (yyyyymmdd)
    # If $3 is present, it should be a calendar type. The default is a
    # fixed 365 day year calendar.
    local day
    local month
    local res=${SUCCESS}
    local year
    local ytd
    local tstr=${1}
    local year0=${2}

    if [ -n "${3}" -a "${3}" != "365" ]; then
        add_error ERR_UNSUPPORT_CAL "" "ERROR: Calendar type, '${3}', not supported"
        res=$?
    else
        # Round up fractional days
        tstr=$(echo "(${tstr} + 0.99999) / 1" | bc --quiet)
        year=$(echo "((${tstr} - 1) / 365) + ${year0}" | bc --quiet)
        day=$(echo "(((${tstr} - 1) % 365) + 1.99999) / 1" | bc --quiet)
        month=12
        for ytd in 334 304 273 243 212 181 151 120 90 59 31; do
            if [ ${day} -gt ${ytd} ]; then
                day=$((day - ytd))
                break
            else
                month=$((month - 1))
            fi
        done
        #
        if [ ${year} -gt 99999 ]; then
            echo $(printf "%06d%02d%02d" ${year} ${month} ${day})
        elif [ ${year} -gt 9999 ]; then
            echo $(printf "%05d%02d%02d" ${year} ${month} ${day})
        else
            echo $(printf "%04d%02d%02d" ${year} ${month} ${day})
        fi
    fi
    return ${res}
}

get_file_date_field() {
    # Given a filename ($1), return its date field. This is the information after
    # the history file number and incorporates the year and optionally the month, day, and time.
    echo "$(echo ${1} | cut -d'.' -f4)"
}

get_hist_file_info() {
    ## Given a path to a history file ($1), return the number of frames and the
    ## array values of a chosen variable ($2) (one for each frame)
    ## $3 is a format string to be used with '-s'
    local fvals
    local res

    if [ ${VERBOSE} -ge 2 ]; then
      qlog "Calling ${ncks} -H -C -v ${2} -s ${3} ${1}"
    fi
    fvals=($(${ncks} -H -C -v ${2} -s ${3} ${1}))
    res=$?
    if [ ${res} -ne 0 ]; then
        add_error ERR_NCKS_MDATA "${1}"                                       \
                  "get_hist_file_info: ERROR ${res} extracting ${2} from test file ${1}"
        return $?
    fi
    echo "${#fvals[@]}:$(echo ${fvals[@]} | tr ' ' ':')"
    return ${SUCCESS}
}

is_yearly_hist_file() {
    ## Given a path to a history file ($1), return 0 if the file appears to
    ##    be a yearly file (date field of filename has only a year).
    ## Return 1 otherwise.

    if [[ "${1}" =~ \.[0-9]{4,5}\.nc$ ]]; then
        return 0
    else
        return 1
    fi
}

is_monthly_hist_file() {
    ## Given a path to a history file ($1), return 0 if the file appears to
    ##    be a monthly file (date field of filename has only a year and month).
    ## Return 1 otherwise.

    if [[ "${1}" =~ \.[0-9]{4,5}[-][0-9]{2}\.nc$ ]]; then
        return 0
    else
        return 1
    fi
}

get_date_from_filename() {
    ## Given a path to a history file ($1), return the date as yyyyxxxx for
    ## yearly history files, yyyymmxx for monthly history files and yyyymmdd
    ## for other files
    local datefield="$(echo ${1} | cut -d'.' -f4)"
    local year="$(echo ${datefield} | cut -d'-' -f1)"
    local month="$(echo ${datefield} | cut -d'-' -f2 -s)"
    local day="$(echo ${datefield} | cut -d'-' -f3 -s)"
    # Fill in xx for day if blank (e.g., for monthly files)
    if [ -z "${month}" ]; then
        month="xx"
    fi
    if [ -z "${day}" ]; then
        day="xx"
    fi

    echo ${year}${month}${day}
}

get_atm_hist_file_info() {
    ## Given a path to an CAM history file ($1), return the number of
    ## frames and the date array values (one for each frame)

    local dates
    local res=${SUCCESS}
    if is_monthly_hist_file "${1}"; then
        dates="1:$(get_date_from_filename ${1})"
    else
        dates=$(get_hist_file_info "${1}" "date" "%d\n")
        res=$?
    fi
    echo ${dates}
    return ${res}
}

get_lnd_hist_file_info() {
    ## Given a path to an CTSM history file ($1), return the number of frames and the
    ## date array values (one for each frame)

    local dates
    local res=${SUCCESS}
    if is_monthly_hist_file "${1}"; then
        dates="1:$(get_date_from_filename ${1})"
    else
        dates=$(get_hist_file_info "${1}" "mcdate" "%d\n")
        res=$?
    fi
    echo ${dates}
    return ${res}
}

get_year0_from_time_attrib() {
    ## Given a history file ($1), return the start year if the time variable
    ## is days since a starting year and the calendar is 'noleap'.
    ## Otherwise, throw an error and return an empty string

    local attrib
    local res
    local tstr
    local year0=""  # Year calendar begins

    if [ ${VERBOSE} -ge 2 ]; then
      qlog "Calling ${ncks} --metadata -C -v time ${1}"
    fi
    attrib="$(${ncks} --metadata -C -v time ${1})"
    res=$?
    if [ ${res} -ne 0 ]; then
        add_error ERR_BAD_YEAR0 "${1}"                                        \
                  "ERROR ${res} extracting time metadata from test file ${1}"
        res=$?
    fi
    if [ ${res} -eq 0 ]; then
        tstr=$(echo "${attrib}" | grep time:calendar)
        if [[ ! "${tstr,,}" =~ "noleap" ]]; then
            add_error ERR_UNSUPPORT_CAL "${1}"                                \
                      "Unsupported time calendar for '${1}', '${tstr}'"
            res=$?
        fi
    fi
    if [ ${res} -eq 0 ]; then
        tstr=$(echo "${attrib}" | grep time:units)
        if [[ ! "${tstr}" =~ days\ since\ ([0-9]{4,})-01-01\ 00:00 ]]; then
            add_error ERR_UNSUPPORT_TIME "${1}"                               \
                      "Unsupported time units for '${1}', '${tstr}'"
            res=$?
        else
            year0="${BASH_REMATCH[1]}"
        fi
    fi
    echo "${year0}"
    if [ ${res} -eq 0 ]; then
        return ${SUCCESS}
    else
        return ${res}
    fi
}

get_ice_hist_file_info() {
    ## Given a path to a CICE history file ($1), return the number of
    ##    frames and the date array values (one for each frame)
    ## CICE (at least CICE5) has time as "days since yyyy-01-01 00:00:00"
    ##    attribute and a noleap calendar. Check these attributes and
    ##    derive the date from the time.

    local times=()
    local res
    local tind
    local year0  # Year calendar begins

    if [ "${USEICEFILENAMES}" == "yes" ]; then
        echo "1:$(get_date_from_filename ${1})"
    else
        ## Get the date(s) from the file
        year0="$(get_year0_from_time_attrib ${1})"
        res=$?
        if [ -n "${ERRMSG}" ]; then
            exit ${ERRCODE}
        fi
        if [ -n "${year0}" ]; then
            tind="$(get_hist_file_info "${1}" "time" "%f\n")"
            res=$?
            if [ ${res} -eq ${SUCCESS} ]; then
                times=(${tind//:/ })
            fi # No else, on error, times should be empty
        fi
        ## Convert times to dates
        for tind in $(seq 1 $((${#times[@]} - 1))); do
            times[${tind}]=$(convert_time_to_date ${times[${tind}]} ${year0})
            res=$?
            if [ ${res} -ne ${SUCCESS} ]; then
                add_fatal_error ERR_UNSUPPORT_CAL ""                          \
                                "get_ice_hist_file_info: unsupported calendar"
                return ${res}
            fi
        done
        echo $(echo "${times[@]}" | tr ' ' ':')
    fi
    return ${res}
}

get_ocn_hist_file_info() {
    ## Given a path to BLOM history file ($1), return the number of frames
    ## and the date array values (one for each frame)
    ## BLOM has time as "days since yyyy-01-01 00:00:00" attribute
    ##    and a noleap calendar. Check these attributes and derive the
    ##    date from the time

    local dates
    local times=()
    local tind
    local year0  # Year calendar begins

    if is_yearly_hist_file "${1}"; then
        dates="1:$(get_date_from_filename ${1})"
    else
        year0="$(get_year0_from_time_attrib ${1})"
        if [ -n "${ERRMSG}" ]; then
            exit ${ERRCODE}
        fi
        if [ -n "${year0}" ]; then
            tind="$(get_hist_file_info "${1}" "time" "%f\n")"
            res=$?
            if [ ${res} -eq ${SUCCESS} ]; then
                times=(${tind//:/ })
            fi # No else, on error, times should be empty
        fi
        ## Convert times to dates
        for tind in $(seq 1 $((${#times[@]} - 1))); do
            times[${tind}]=$(convert_time_to_date ${times[${tind}]} ${year0})
            res=$?
            if [ ${res} -ne ${SUCCESS} ]; then
                add_fatal_error ERR_UNSUPPORT_CAL ""                          \
                                "get_ocn_hist_file_info: unsupported calendar"
                return ${res}
            fi
        done
        dates="$(echo ${times[@]} | tr ' ' ':')"
    fi
    echo ${dates}
    return ${res}
}

get_rof_hist_file_info() {
    ## Given a path to an MOSART history file ($1), return the number of
    ## frames and the date array values (one for each frame)

    local dates
    local res=${SUCCESS}
    if is_yearly_hist_file "${1}"; then
        dates="1:$(get_date_from_filename ${1})"
    elif is_monthly_hist_file "${1}"; then
        dates="1:$(get_date_from_filename ${1})"
    else
        dates=$(get_hist_file_info "${1}" "mcdate" "%d\n")
        res=$?
    fi
    echo "${dates}"
    return ${res}
}

greater_than() {
    # Return zero if $1 > $2, one otherwise
    if [ -z "${1}" ]; then
        add_error ERR_BADARG_GT ""                                            \
                  "greater_than requires two arguments, was called with none"
        return $?
    elif [ -z "${2}" ]; then
        add_error ERR_BADARG_GT ""                                            \
                  "greater_than requires two arguments, was called with '${1}'"
        return $?
    fi
    bcval="$(echo "${1} > ${2}" | bc --quiet)"
    if [ -z "${bcval}" ]; then
        add_error ERR_BADARG_GT ""                                            \
                  "Bad bc call in greater_than: echo \"${1} > ${2}\" | bc --quiet"
        return $?
    fi
    return $((1 - bcval));
}

less_than() {
    # Return zero if $1 < $2, one otherwise
    local bcval # Output from bc
    if [ -z "${1}" ]; then
        add_error ERR_BADARG_LT ""                                            \
                  "less_than requires two arguments, was called with none"
        return $?
    elif [ -z "${2}" ]; then
        add_error ERR_BADARG_LT ""                                            \
                  "less_than requires two arguments, was called with '${1}'"
        return $?
    fi
    bcval="$(echo "${1} < ${2}" | bc --quiet)"
    if [ -z "${bcval}" ]; then
        add_error ERR_BADARG_LT ""                                            \
                  "Bad bc call in less_than: echo \"${1} < ${2}\" | bc --quiet"
        return $?
    fi
    return $((1 - bcval));
}

bnds_from_array() {
    ## Return the minimum and maximum values in the input array
    local minval=""
    local maxval=""
    local res

    for frame in $@; do
        if [ -z "${minval}" ]; then
            minval="${frame}"
        fi
        if [ -z "${maxval}" ]; then
            maxval="${frame}"
        fi
        less_than ${frame} ${minval}
        res=$?
        if [ ${res} -eq 0 ]; then
            minval="${frame}"
        elif [ ${res} -gt ${SUCCESS} ]; then
            return ${res}
        fi
        greater_than ${frame} ${minval}
        res=$?
        if [ ${res} -eq 0 ]; then
            maxval="${frame}"
        elif [ ${res} -gt ${SUCCESS} ]; then
            return ${res}
        fi
    done
    echo "${minval},${maxval}"
    return ${SUCCESS}
}

get_year_from_date() {
    ## Given a date string, return the year
    local res=${SUCCESS}
    if [ ${#1} -lt 8 ]; then
        add_error ERR_BAD_DATESTR ""                                          \
                  "get_year_from_date; Bad date string, '${1}'"
        res=$?
    else
        echo "${1:0:-4}"
    fi
    return ${res}
}

get_month_from_date() {
    ## Given a date string, return the month
    local res=${SUCCESS}
    if [ ${#1} -lt 8 ]; then
        add_error ERR_BAD_DATESTR ""                                          \
                  "get_month_from_date; Bad date string, '${1}'"
        res=$?
    else
        echo "${1:${#1}-4:-2}"
    fi
    return ${res}
}

get_day_from_date() {
    ## Given a date string, return the day of the month
    local res=${SUCCESS}
    if [ ${#1} -lt 8 ]; then
        add_error ERR_BAD_DATESTR ""                                          \
                  "get_day_from_date; Bad date string, '${1}'"
        res=$?
    fi
    echo "${1:${#1}-2}"
    return ${res}
}

get_range_year() {
    ## Given a "date string" from get_xxx_hist_file_info, return
    ## the year of all the dates or an error if more than one year
    ## is found.
    ## $1 is the date string, $2 is a filename for an error message
    local datestr
    local file="${2}"
    local res=${SUCCESS}
    local year=-1
    local tyear
    if [ -z "${file}" ]; then
        file="file"
    fi
    for datestr in $(echo ${1} | cut -d':' -f2- | tr ':' ' '); do
        tyear="$(get_year_from_date ${datestr})"
        res=$?
        if [ ${res} -ne ${SUCCESS} ]; then
            break
        fi
        if [ ${year} -lt 0 ]; then
            year="${tyear}"
        elif [[ ! "${tyear}" =~ ^[0-9]+$ ]]; then
            add_error ERR_BADYEAR "${2}"                                      \
                      "get_range_year: Invalid year found in ${file}"
            res=$?
            year="ERROR"
            break
        elif [ "${year}" != "${tyear}" ]; then
            add_error ERR_MULTYEARS "${2}"                                    \
                      "get_range_year: Multiple years found in ${file}"
            res=$?
            year="ERROR"
            break
        fi
    done
    echo "${year}"
    return ${res}
}

get_range_month() {
    ## Given a "date string" from get_xxx_hist_file_info, return
    ## the year:month of all the dates or an error if more than one month
    ## is found.
    ## $1 is the date string, $2 is a filename for an error message
    local datestr
    local file="${2}"
    local month=-1
    local res=${SUCCESS}
    local tmonth
    local year=-1
    local tyear
    if [ -z "${file}" ]; then
        file="file"
    fi
    for datestr in $(echo ${1} | cut -d':' -f2- | tr ':' ' '); do
        tyear="$(get_year_from_date ${datestr})"
        res=$?
        if [ ${res} -ne ${SUCCESS} ]; then
            break
        fi
        tmonth="$(get_month_from_date ${datestr})"
        res=$?
        if [ ${res} -ne ${SUCCESS} ]; then
            break
        fi
        if [ ${year} -lt 0 ]; then
            year="${tyear}"
            if [ ${month} -ge 0 ]; then
                add_error ERR_INTERNAL "${2}"                                 \
                          "get_range_month: Internal error, month = ${month}"
                res=$?
                break
            else
                month="${tmonth}"
            fi
        elif [ ${month} -lt 0 ]; then
            add_error ERR_INTERNAL "${2}"                                     \
                      "get_range_month: Internal error, year = ${year}"
            res=$?
            break
        elif [[ ! "${tyear}" =~ ^[0-9]+$ ]]; then
            add_error ERR_INTERNAL "${2}"                                     \
                      "get_range_month: Invalid year found in ${file}"
            res=$?
            break
        elif [[ ! "${tmonth}" =~ ^[0-9]+$ ]]; then
            add_error ERR_INTERNAL "${2}"                                     \
                      "get_range_month: Invalid month found in ${file}"
            res=$?
            break
        elif [ "${year}" != "${tyear}" ]; then
            add_error ERR_INTERNAL "${2}"                                     \
                      "get_range_month: Multiple years found in ${file}"
            res=$?
            break
        elif [ "${month}" != "${tmonth}" ]; then
            add_error ERR_INTERNAL "${2}"                                     \
                      "get_range_month: Multiple months found in ${file}"
            res=$?
            break
        fi
    done
    if [ ${res} -eq ${SUCCESS} ]; then
        echo "${year}:${month}"
    fi
    return ${res}
}

get_file_date() {
    ## Given a history file ($1), a component type ($2) and a merge type ($3),
    ## find the applicable date in the file or generate an error.
    ## The function is only valid for the 'yearly' and 'monthly' merge types.
    ## mergeall merges return the date of the file as yyyymmdd
    ## compressonly returns the date / time field of the file
    local hfile="${1}"
    local comp="${2}"
    local merge="${3}"
    local res=${SUCCESS}
    local tdate=""
    local tdates=""
    if [ ! -f "${hfile}" ]; then
        add_error ERR_MISSING_FILE "${hfile}"                                 \
                  "get_file_date: File does not exist, '${hfile}'"
        res=$?
    elif [ "${merge}" == "mergeall" ]; then
        tdates="$(echo ${hfile} | cut -d'.' -f4 | cut -d'-' -f1-3 | tr -d '-')"
    elif [ "${merge}" == "compressonly" ]; then
        tdates="$(echo ${hfile} | cut -d'.' -f4)"
    elif [ "${merge}" != "yearly" -a "${merge}" != "monthly" ]; then
        add_error ERR_BAD_MERGETYPE "${hfile}"                                \
                  "get_file_date: Unrecognized merge type, '${merge}'"
        res=$?
    else
        if [ "${comp}" == "atm" ]; then
            tdates="$(get_atm_hist_file_info ${hfile})"
            res=$?
        elif [ "${comp}" == "ice" ]; then
            tdates="$(get_ice_hist_file_info ${hfile})"
            res=$?
        elif [ "${comp}" == "lnd" ]; then
            tdates="$(get_lnd_hist_file_info ${hfile})"
            res=$?
        elif [ "${comp}" == "ocn" ]; then
            tdates="$(get_ocn_hist_file_info ${hfile})"
            res=$?
        elif [ "${comp}" == "rof" ]; then
            tdates="$(get_rof_hist_file_info ${hfile})"
            res=$?
        else
            add_error ERR_BAD_COMPTYPE "${hfile}"                             \
                      "get_file_date: Unrecognized component type, '${comp}'"
            res=$?
        fi
    fi
    if [ -n "${tdates}" ]; then
        if [ "${merge}" == "yearly" ]; then
            tdate="$(get_range_year ${tdates} ${hfile})"
            res=$?
        elif [ "${merge}" == "monthly" ]; then
            tdate="$(get_range_month ${tdates} ${hfile})"
            res=$?
        else
            tdate="${tdates}"
        fi
        echo "${tdate}"
    fi
    return ${res}
}

get_xxhsum_filename() {
    ## Given a filename or directory ($1), return the name for the xxhsum
    ## filename to use for compression jobs in that directory.
    local cdir=""  # The name of the directory where $1 is located
    local fname="" # The xxhsum filename
    local pdir     # Parent directory name
    if [ -f "${1}" ]; then
        cdir="$(realpath $(dirname ${1}))"
    elif [ -d "$(realpath ${1})" ]; then
        cdir="$(realpath ${1})"
    elif [ "${DRYRUN}" == "yes" ]; then
        ## For DRYRUN, pretend $1 is a directory
        cdir="$(realpath ${1})"
    else
        ERRMSG="get_xxhsum_filename: Invalid filename or directory input, '${1}'"
        errlog "${ERRMSG}"
        ERRCODE=${ERR_INTERNAL}
        return ${ERRCODE}
    fi
    if [ -n "${cdir}" ]; then
        if [ "$(basename ${cdir})" == "hist" ]; then
            ## We are in a component directory, grab the component name
            ## and the casedir name (above component)
            pdir="$(dirname ${cdir})" # e.g., ice, ocn
            fname="$(basename $(dirname ${pdir}))_$(basename ${pdir})"
        elif [ -f "${1}" ]; then
            ## Take the case name from the filename
            fname=$(echo $(basename "${1}") | cut -d'.' -f1)
        else
            ## Just take the name of the directory
            fname="$(basename ${cdir})"
        fi
        fname="${cdir}/${fname}_${JOBLID}.xxhsum"
        if [ ! -f "${fname}" -a -d "${cdir}" ]; then
            touch ${fname}
        fi
        echo "${fname}"
        return ${SUCCESS}
    else
        ERRMSG="get_xxhsum_filename: Invalid filename or directory input, '${1}'"
        errlog "${ERRMSG}"
        ERRCODE=${ERR_INTERNAL}
        return ${ERRCODE}
    fi
}

compare_frames() {
    ## Compare the output file ($1) with some of the corresponding
    ## input files ($4-)
    ## $2 is the component type (e.g., atm, ice)
    ## $3 is a unique job number to allow thread-safe temporary filenames
    ## Return SUCCESS or an error code
    local outfile=${1}
    local comp=${2}
    local job_num=${3}
    shift 3
    local files=($@)
    local check_files              # List of source file indices to check
    local diff_output              # cprnc output to parse
    local diff_title               # First part of filename for cprnc output
    local emsg                     # Error message temp
    local endmsg                   # File checking message
    local ftimes                   # Array of time (or date) fields from an input file
    local nco_args                 # Inputs for the next NCO call
    local -i nfail                 # Number of failed frame checks
    local -i numfiles=${#files[@]} # Number of input files
    local -i num_check_files       # Number of source files to check
    local pass                     # Var used to test cprnc pass / fail
    local passmsg="."              # Pass / Fail message
    local pl=""                    # 's' for multiple test frames
    local res=${SUCCESS}           # Test if last command succeeded (zero return)
    local sfile                    # Source file currently being checked
    local test_filename            # Unique temp filename for extracted frames
    local timevar                  # Variable name containing the time (or date) information

    test_filename="${outpath}/test_frame_j${job_num}_$(date +'%Y%m%d%H%M%S').nc"
    if [ -f "${test_filename}" ]; then
        # This should not happen!
        add_fatal_error ERR_INTERNAL "${outfile}"                              \
                        "Temp filename, '${test_filename}', already exists"
        return  $?
    fi
    if [ "${COMPARE}" == "Spot" ]; then
        num_check_files=$(((${numfiles} + 7) / 10)) # Plus first and last source file
        if [ ${numfiles} -gt 0 ]; then
            check_files=(1)
        fi
        for snum in $(seq ${num_check_files}); do
            check_files+=($((snum*(numfiles + 1) / (num_check_files + 1))))
        done
        if [ ${numfiles} -gt 1 ]; then
            check_files+=($numfiles)
        fi
        log "Spot checking ${#check_files[@]} source files against the corresponding frame(s) from ${outfile}"
        endmsg="Done spot checking ${outfile} against selected source files"
        passmsg=", all PASS."
        job_status[${outfile}]="in spot check"
    elif [ "${COMPARE}" == "Full" ]; then
        check_frames=($(seq ${numfiles}))
        log "Checking each source file against the corresponding frame(s) from ${outfile}."
        endmsg="Done checking all frames from ${outfile} against the corresponding source files"
        passmsg=", all PASS."
        job_status[${outfile}]="in full check"
    else
        check_frames=()
        endmsg="Skipping source file check"
        job_status[${outfile}]="pass"
    fi
    diff_title="cprnc_diff_frame_j${job_num}.$(echo $(basename ${outfile}) | cut -d'.' -f3-4)"
    for check_file in ${check_files[@]}; do
        ## Find the source filename for this check
        sfile=${files[${check_file}-1]}
        if [ -z "${sfile}" ]; then
            emsg="empty entry in compare_frames (\${files[${check_file}-1]})"
            log "INTERNAL ERROR: check_files=(${check_files[@]})"
            log "INTERNAL ERROR: files=(${files[@]})"
            add_fatal_error ERR_INTERNAL "${outfile}" "${emsg}"
            return $?
        elif [ ! -f "${sfile}" ]; then
            emsg="file in compare_frames, '${files[${check_file}-1]}', does not exist"
            add_fatal_error INTERNAL ERROR "${outfile}" "${emsg}"
            return $?
        fi
        ## Extract the time dimension for this file
        timevar="time"
        nco_args="-s %f: -H -C -v ${timevar} ${sfile}"
        if [ ${VERBOSE} -ge 2 ]; then
            qlog "Calling ${ncks} ${nco_args}"
        fi
        ftimes=($(echo $(${ncks} ${nco_args}) | tr ':' ' '))
        res=$?
        if [ ${res} -ne 0 ]; then
            emsg="${res} extracting time from test file ${check_file}"
            add_error ERR_BAD_TIME "${outfile}" "${emsg}"
            return $?
        fi
        if [ ${#ftimes[@]} -gt 1 ]; then
            pl="s"
        else
            pl=""
        fi
        ## Extract the matching from the output file
        ## For now, set the stride to one.
        ## One could add an option here to only spot check frames,
        ##    however, that means pulling frames out of the source
        ##    file which takes time and space.
        bnds_str="$(bnds_from_array ${ftimes[@]}),1"
        res=$?
        if [ ${res} -ne ${SUCCESS} ]; then
            add_error ${res} "${outfile}" "bad frame bounds from '${ftimes[@]}'"
            return $?
        fi
        nco_args="-d ${timevar},${bnds_str} ${outfile} ${test_filename}"
        if [ "${DRYRUN}" == "yes" ]; then
            log "Calling: ${ncks} ${nco_args}"
        else
            if [ ${VERBOSE} -ge 2 ]; then
                qlog "Calling ${ncks} ${nco_args}"
            fi
            ${ncks} ${nco_args}
            res=$?
            if [ ${res} -ne 0 ]; then
                ## Cleanup
                rm -f ${test_filename}
                add_error ERR_EXTRACT "${outfile}"                            \
                          "${res} extracting test frame${pl} from output file"
                return $?
            fi
        fi
        ## Run cprnc to test output frame against input file
        diff_output="${outpath}/${diff_title}.f${check_file}_$(date +'%Y%m%d%H%M%S').txt"
        if [ -f "${diff_output}" ]; then
            # Yeah, this should not be necessary but . . .
            rm -f ${diff_output}
        fi
        if [ "${DRYRUN}" == "yes" ]; then
            log "cprnc ${sfile} ${test_filename} > ${diff_output}"
        else
            if [ ${VERBOSE} -ge 2 ]; then
                qlog "Calling ${cprnc} ${sfile} ${test_filename} > ${diff_output}"
            fi
            ${cprnc} ${sfile} ${test_filename} > ${diff_output}
            res=$?
            if [ ${res} -ne 0 ]; then
                ## Cleanup
                rm -f ${test_filename}
                add_error ERR_CPRNC "${outfile}"                              \
                          "${res} running cprnc to verify output frame${pl} from file, ${sfile}"
                return $?
            fi
            grep 'diff_test' ${diff_output} | grep --quiet IDENTICAL
            pass=$?
            if [ $pass -eq 0 ]; then
                log "Checking ${sfile} against output frame${pl} . . . PASS"
            else
                # Log the comparison failure but do not return an error
                nfail=$((nfail + 1))
                add_error ERR_COMPARE "${outfile}"                            \
                          "Checking ${sfile} against output frame${pl} . . . FAIL"
                res=$?
                log "cprnc output saved in ${diff_output}"
            fi
        fi
        ## Cleanup
        rm -f ${test_filename}
        if [ $pass -eq 0 ]; then
            rm -f ${diff_output}
        fi
    done
    if [ ${nfail} -gt 0 ]; then
        passmsg=", ${nfail} comparison FAILures."
    else
      job_status[${outfile}]="pass"
    fi
    fail_count[${outfile}]=${nfail}
    log "${endmsg}${passmsg}"
    return ${res}
}

create_checksum() {
    # Checksum an input file and add it to the correct checksum log file
    # $1 is the file to be checksummed
    # Return SUCCESS on success, otherwise, return an error code
    local checkfile="${1}"
    local res
    local xxhsumfile

    if [ "${DRYRUN}" == "yes" ]; then
        log "${xxhsum} -H2 ${outfile} >> \${xxhsumfile}"
    else
        xxhsumfile=$(get_xxhsum_filename ${checkfile})
        res=$?
        if [ ${res} -ne ${SUCCESS} ]; then
            # Need to define error message and code since func was called in subshell
            add_error ERR_INTERNAL "${checkfile}" "${xxhsumfile}"
            res=$?
        else
            ${xxhsum} -H2 ${checkfile} >> ${xxhsumfile}
            res=$?
            if [ ${res} -ne 0 ]; then
                add_error ERR_INTERNAL "${checkfile}"                         \
                          "ERROR ${res} running xxhsum on ${checkfile}"
                res=$?
            fi
        fi
    fi
    return ${res}
}

copy_or_compress_file() {
   # Compress $1 if it is a NetCDF file, otherwise just copy it.
   # $1 is the file to copy or compress
   # $2 is the destination directory.
   # Return SUCCESS if the operation is successful, otherwise, return an error
   local hfile="${1}"
   local outdir="${2}"
   local outfile="${outdir}/${hfile}"
   local -i res=0
   if [ "${hfile: -3}" != ".nc" ]; then
      if [ "${DRYRUN}" == "yes" ]; then
         log "Dryrun, copy '${hfile}' to '${outdir}'"
      else
         job_status[${outfile}]="in copy"
         if [ ${VERBOSE} -ge 1 ]; then
            qlog "Copying '${hfile}' to '${outdir}'"
         fi
         cp ${hfile} ${outfile}
         res=$?
      fi
      if [ ${res} -eq 0 ]; then
         job_status[${outfile}]="copied"
      else
          add_error ERR_COPY "${outfile}" "Copying '${hfile}' to '${outdir}', res = ${res}"
         return $?
      fi
   else
      if [ "${DRYRUN}" == "yes" ]; then
        log "${ncrcat} -O -4 -L ${COMPRESS} ${hfile} -o ${outfile}"
    else
        if [ ${VERBOSE} -ge 2 ]; then
            qlog "Calling: ${ncrcat} -O -4 -L ${COMPRESS} ${hfile} -o ${outfile}"
        fi
        ${ncrcat} -O -4 -L ${COMPRESS} ${hfile} -o ${outfile}
        res=$?
        if [ ${res} -ne 0 ]; then
            add_error ERR_NCRCAT "${outfile}" "Error ${res} compressing ${hfile}"
            return $?
        fi
      fi
   fi
   if [ ${res} -eq 0 ]; then
      create_checksum ${outfile}
      res=$?
   fi
   if [ ${res} -eq 0 ]; then
      return ${SUCCESS}
   else
      return ${res}
   fi
}

convert_cmd() {
    ## Compress files ($4-) into a single file, $1.
    ## $3 is the model type (e.g., atm, lnd)
    ## $4 is a unique job number to allow thread-safe temporary filenames
    ## Return
    local outfile=${1}
    local comp=${2}
    local job_num=${3}
    shift 3
    local files=($@)
    local nfil
    local numfiles="${#files[@]}"
    local reffile="${files[-1]}"
    local res
    local retcode=${SUCCESS}
    local vmsg

    if [ ${VERBOSE} -ge 1 ]; then
        vmsg="Concatenating ${#files[@]} to ${outfile} using level ${COMPRESS} compression"
        vmsg="${vmsg}\nFiles to concatenate are:\n$(echo ${files[@]} | tr ' ' '\n')"
        qlog -e "${vmsg}"
    fi
    job_status[${outfile}]="in compress"
    if [ ${#files[@]} -eq 0 ]; then
        ERRMSG="INTERNAL ERROR: No files to compress to '${outfile}'?"
        error_reports[${outfile}]="${ERRMSG}"
        errlog "${ERRMSG}"
        ERRCODE=${ERR_NOCOMPRESS}
        retcode=${ERRCODE}
        job_status[${outfile}]="ERROR"
        fail_count[${outfile}]=${ERRCODE}
    elif [ "${DRYRUN}" == "yes" ]; then
        log "${ncrcat} -O -4 -L ${COMPRESS} ${files[@]} -o ${outfile}"
    else
      if [ ${VERBOSE} -ge 2 ]; then
        qlog "Calling: ${ncrcat} -O -4 -L ${COMPRESS} ${files[@]} -o ${outfile}"
      fi
        ${ncrcat} -O -4 -L ${COMPRESS} ${files[@]} -o ${outfile}
        res=$?
        if [ ${res} -ne 0 ]; then
            ERRMSG="ERROR ${res} concatenating ${files[@]}"
            errlog "${ERRMSG}"
            error_reports[${outfile}]="${ERRMSG}"
            ERRCODE=${ERR_NCRCAT}
            retcode=${ERRCODE}
            job_status[${outfile}]="ERROR"
            fail_count[${outfile}]=${ERRCODE}
        fi
    fi
    if [ ${retcode} -eq ${SUCCESS} ]; then
        job_status[${outfile}]="compressed"
        fail_count[${outfile}]=${SUCCESS}
    fi
    if [ ${retcode} -eq ${SUCCESS} ]; then
        touch -r ${reffile} ${outfile}
        create_checksum "${outfile}"
        retcode=$?
        if [ ${numfiles} -eq 1 ]; then
            nfil="file"
        else
            nfil="files"
        fi
    fi
    if [ ${retcode} -eq ${SUCCESS} ]; then
        if [ "${DRYRUN}" == "yes" ]; then
            log "DRYRUN: $(basename ${outfile}): ${numfiles} ${nfil} merged"
        else
            log "DONE: $(basename ${outfile}): ${numfiles} ${nfil} merged"
        fi
        compare_frames "${outfile}" ${comp} ${job_num} ${files[@]}
        res=$?
        if [ ${res} -ne ${SUCCESS} ]; then
            error_reports[${outfile}]="${ERRMSG}"
            ERRCODE=${ERR_COMPARE}
            retcode=${ERRCODE}
            job_status[${outfile}]="ERROR"
            fail_count[${outfile}]=${ERRCODE}
        elif [ ${MOVE} -eq 1 ]; then
            if [ "${DRYRUN}" == "yes" ]; then
                log "Not moving source files (DRYRUN)"
            else
                mv ${files[@]} ${MOVEDIR}
            fi
        fi # No else, compare was successful but no move happening
    fi
    return ${retcode}
}

convert_loop() {
    # Loop through components and concatenate its history files
    # Takes a single argument, a log file for echoing output
    local atemp          # Temp variable
    local cname          # Loop index
    local comp           # Current component name (e.g., atm, ice)
    local comparr        # Temp array
    local comppath       # Path to component files to compress
    local currdir="$(pwd -P)"
    local -A dates       # Array keyed by date (year or year:month) with a list of files to process
    local fdate          # The date field of a file
    local file_list      # The current list of files to compress
    local hfile          # Loop index
    local hist_files=()  # List of all history files found
    local hpatt          # Component type dependent file matching
    local -i job_num=0   # Current compression job
    local -i maxdate     # Temp date variable
    local -i mindate     # Temp date variable
    local mod            # The name of the model (e.g., cam, cice)
    local msg            # For constructing log messages
    local -i nexttime    # Keep track of the next time to display waiting message
    local -i nfails=0    # Figure out if any job has failed.
    local -i nfileproc=0 # Number of files cataloged so far
    local -i nhfiles     # Number of history files to process
    local -i njobs       # Current number of running jobs
    local outfile        # Filename for compressed output file
    local outdir         # Location of compressed files
    local retcode        # Return code from previous call
    local rdir           # Loop variable
    local vtemp          # Temp variable
    local tdate          # File date field part of file list key
    if  [ ${#COMPONENTS[@]} -eq 0 ]; then
        COMPONENTS+=("ice:cice")
    fi
    for component in ${COMPONENTS[@]}; do
        comparr=(${component//:/ })
        comp=${comparr[0]}
        mod=${comparr[1]}
        if [ -z "${mod}" -a "${comp}" == "rest" ]; then
            mod="${comp}"
        fi
        if [ "${comp}" == "rest" ]; then
            outdir="${outpath}/${comp}"
            comppath="${casepath}/${comp}"
        else
            outdir="${outpath}/${comp}/hist"
            comppath="${casepath}/${comp}/hist"
        fi
        if [ ! -d "${outdir}" ]; then
            mkdir -p "${outdir}"
        fi
        case ${comp} in
        atm) hpatt="[.]h[0-9]\{1,2\}";;
        lnd) hpatt="[.]h[0-9]\{1,2\}";;
        ice) hpatt="[.]h[0-9]\{0,2\}";;
        ocn) hpatt="[.]h[a-z]\{1,4\}";;
        rof) hpatt="[.]h[0-9]\{1,2\}";;
        rest) hpatt=".*";;
        default)
            add_fatal_error ERR_INTERNAL "" "Unknown component, '${comp}'"
            ;;
        esac
        if [ ! -d "${comppath}" ]; then
            log "WARNING: case path, '${comppath}', not found, skipping"
            continue
        fi
        cd ${comppath}
        res=$?
        if [ ${res} -ne 0 ]; then
            add_error ERR_NOACCESS ""                                        \
                      "Cannot access case directory, '${comppath}'"
            continue
        fi
        log "--------------------"
        if [ "${mod}" == "rest" ]; then
            hist_files=($(ls */*))
            for rdir in $(ls); do
                # Create restart directories
                mkdir ${outdir}/${rdir}
            done
        else
            hist_files=($(ls | grep -e "${casename}[.]${mod}.*${hpatt}.*[.]nc$"))
        fi
        nhfiles=${#hist_files[@]}
        log "${comp} hist files: ${nhfiles}"
        # Create a dictionary of every matching file. Key is the date field
        #  plus the type of history file, the value is filename.
        # We assume that all file sets encompass the same dates.
        # Also, gather all the dates (years or year:month pairs)
        dates=()
        log "Cataloging history files for ${comp}\n"
        for hfile in ${hist_files[@]}; do
            if [ "${hfile: -3}" != ".nc" -o "${comp}" == "rest" ]; then
                # Escape clause for restart directories
                # Just copy each file that does not appear to be a NetCDF file
                copy_or_compress_file ${hfile} ${outdir}
                retcode=$?
                if [ ${retcode} -ne ${SUCCESS} ]; then
                    break
                fi
                ((job_num++))
                nfileproc=$((nfileproc + 1))
                report_progress ${nfileproc} ${nhfiles} ${PROGREPINT}         \
                                "" " files cataloged"
                continue
            fi
            cname="$(get_file_set_name ${hfile})"
            vtemp="${MERGETYPE}"
            if [ "${KEEP_MONTHLY}" == "yes" ]; then
                if is_monthly_hist_file "${hfile}"; then
                    # Override the date to keep monthly file from being concatenated
                    # Note that this clause should only happen if MERGETYPE==yearly
                    vtemp="monthly"
                fi
            fi
            fdate="$(get_file_date ${hfile} ${comp} ${vtemp})"
            retcode=$?
            if [ ${retcode} -ne ${SUCCESS} ]; then
                break
            fi
            if [ -z "${fdate}" -o -n "$(echo ${fdate} | grep [^0-9:])" ]; then
                add_error ERR_BAD_DATESTR "${hfile}"                          \
                          "convert_loop: Bad date from '${hfile}', '${fdate}'"
                break
            fi
            tdate="${cname};${fdate}"
            if [ "${MERGETYPE}" == "yearly" -o "${MERGETYPE}" == "monthly" ]; then
                # Add the history file type here (after error check)
                if [ -n "dates[${tdate}]" ]; then
                    dates["${tdate}"]="${dates[${tdate}]}:${hfile}"
                else
                    dates["${tdate}"]="${hfile}"
                fi
            elif [ "${MERGETYPE}" == "mergeall" ]; then
                # One list for each cname, keep track of oldest and newest dates
                # tdate is yyyymmdd-YYYYMMDD where:
                #    yyyymmdd is the oldest file to concatenate
                #    YYYYMMDD is the newest file to concatenate
                vtemp=$(echo ${tdate} | cut -d';' -f2) # date string
                if [ -n "${dates[${cname}]}" ]; then
                    atemp=(${dates[${cname}]//;/ })
                    mindate=${atemp[0]}
                    maxdate=${atemp[1]}
                    if [ ${vtemp} -lt ${mindate} ]; then
                        mindate=${vtemp}
                    fi
                    if [ ${vtemp} -gt ${maxdate} ]; then
                        maxdate=${vtemp}
                    fi
                    dates["${cname}"]="${mindate};${maxdate};${atemp[2]}:${hfile}"
                else
                    dates["${cname}"]="${vtemp};${vtemp};${hfile}"
                fi
            elif [ "${MERGETYPE}" == "compressonly" ]; then
                # One file per key. It is an error to have more than one.
                if [ -n "dates[${tdate}]" ]; then
                    add_fatal_error ERR_INTERNAL "${hfile}"                   \
                                    "key clash for '${tdate}'"
                    retcode=$?
                    break
                else
                    dates["${tdate}"]="${hfile}"
                fi
            else
                # This really should not happen!
                add_fatal_error ERR_INTERNAL "${hfile}"                       \
                                "Undefined merge type, '${MERGETYPE}'."
                retcode=$?
            fi
            nfileproc=$((nfileproc + 1))
            report_progress ${nfileproc} ${nhfiles} ${PROGREPINT} "" " files cataloged"
            if fatal_error; then
                break
            fi
        done
        if fatal_error; then
            return ${retcode}
        fi
        for tdate in ${!dates[@]}; do
            ((job_num++))
            file_list=(${dates[${tdate}]//:/ })
            cname="$(echo ${tdate} | cut -d';' -f1)"
            if [ "${MERGETYPE}" == "compressonly" -o "${comp}" == "rest" ]; then
                # Note, this has to be first to capture restart files
                tdate="$(echo ${tdate} | cut -d';' -f2)"
                tdate="${tdate//:/-}"
                outfile="${outdir}/${casename}.${cname}.${tdate}.nc"
            elif [ "${MERGETYPE}" == "yearly" -o "${MERGETYPE}" == "monthly" ]; then
                tdate="$(echo ${tdate} | cut -d';' -f2)"
                tdate="${tdate//:/-}"
                outfile="${outdir}/${casename}.${cname}.${tdate}.nc"
                log "Compressing ${cname} for ${tdate}"
            elif [ "${MERGETYPE}" == "mergeall" ]; then
                # Note that the first file has the date range prepended to it.
                vtemp=(${file_list[0]//;/ })
                tdate="${vtemp[0]}-${vtemp[1]}"
                file_list[0]=${vtemp[2]}
                outfile="${outdir}/${casename}.${cname}.${tdate}.nc"
            else
                # This really should not happen!
                add_fatal_error ERR_INTERNAL "${hfile}"                       \
                                "Undefined merge type, '${MERGETYPE}'."
                retcode=$?
                break
            fi
            msg="$(printf "%4d: Compressing to $(basename ${outfile})\n" ${job_num})"
            if [ ${#file_list[@]} -eq 0 ]; then
                log "No files to compress for ${outfile}, skipping"
                job_status[${outfile}]="skipped (should not happen?)"
            elif [ ${NTHREADS} -le 1 ]; then
                log "${msg}"
                job_status[${outfile}]="created"
                convert_cmd ${outfile} ${comp} ${job_num} ${file_list[@]}
                retcode=$?
                if [ ${retcode} -ne ${SUCCESS} ]; then
                    exit ${ERRCODE}
                fi
            else
                nexttime=$(($(date +%s)))
                while :; do
                    ## Wait to launch a new conversion until the number of jobs is low enough.
                    njobs=$(jobs -r | wc -l)
                    if [ ${njobs} -lt ${NTHREADS} ]; then
                        if fatal_error; then
                            break
                        fi
                        log "${msg}"
                        job_status[${outfile}]="created"
                        convert_cmd ${outfile} ${comp} ${job_num} ${file_list[@]} &
                        break
                    elif [ ${VERBOSE} -ge 2 -a $(($(date +s))) -gt ${nexttime} ]; then
                        log "Waiting for job thread, currently running ${njobs} / ${NTHREADS}"
                        nexttime=$(($(date +%s) + 60))
                    fi
                    sleep 0.5s
                done
                if fatal_error; then
                    break
                fi
            fi
        done
        if fatal_error; then
            break
        fi
        cd ${currdir}
    done
    # Make sure all jobs have finished
    if [ ${NTHREADS} -gt 1 ]; then
        log "Waiting for jobs to finish"
        if [ ${VERBOSE} -ge 1 ]; then
            log "$(jobs)"
        fi
    fi
    wait
    if ! fatal_error; then
        log "${tool} : completed"
    fi
    # Report on jobs run and any errors
    report_job_status 0 ${job_num}
}

if [ $MOVE -eq 1 -a "${UNITTESTMODE}" == "no" ]; then
    if [ "${DRYRUN}" == "yes" ]; then
        log "Not moving source files (DRYRUN)"
    else
        mkdir -p ${MOVEDIR}
    fi
fi

if [ "${DRYRUN}" == "yes" -a "${UNITTESTMODE}" == "no" ]; then
    log "Dry Run, no data files will be created, moved, modified, or deleted."
fi
if [ "${UNITTESTMODE}" == "no" ]; then
    convert_loop
    res=$?
    if [ ${res} -ne ${SUCCESS} -o -n "${ERRMSG}" ]; then
        exit ${ERRCODE}
    fi
fi

if [ ${DELETE} -eq 1 -a "${UNITTESTMODE}" == "no" ];then
    if [ "${DRYRUN}" == "yes" ]; then
        log "Not deleting source files (DRYRUN)"
    else
        printf "Finalize: DELETING the source files in N seconds... "
        for ind in {30..0..-1}; do
            printf "%02d\b\b" $ind; sleep 1
        done
        rm -rf ${MOVEDIR}
    fi
fi
