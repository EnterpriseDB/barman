#!/usr/bin/env bash

: <<LICENSE
Copyright (c) 2016, Finalsite, LLC.  All rights reserved.
Copyright (c) 2016, Darryl Wisneski <darryl.wisneski@finalsite.com>

Redistribution and use in source and binary forms, with or without 
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

LICENSE

PROGRAM="$(/bin/basename "$0")"
REVISION="0.7.0"

STATE_OK=0
STATE_CRITICAL=2
STATE_UNKNOWN=3

DATE="$(date +%Y%m%d)"
AUTO_RECOVERY_PATH="/var/lib/barman/auto_recovery"
MANUAL_RECOVERY_PATH="/var/lib/barman/recovery"
AUTO_RECOVERY_PORT="5433"
AUTO_RECOVERY_APPNAME="composer"

revision_details() {
  echo "$1 v$2"
  return 0
}

usage() {
    cat <<EOF
 Usage:
    ${PROGRAM} [options]

Options:
    -a --app-name          Barman server/app name (default: ${AUTO_RECOVERY_APPNAME})
    -b --backup-name       Barman backup name to recover
    -h --help              Show usage information and exit
    -m --manual            Manual mode (default: ${MANUAL_RECOVERY_PATH})
    -p --port              Listen port for PG recovery DB (default: ${AUTO_RECOVERY_PORT})
    -l --list-backups      List barman backups
    -r --auto              Automated mode (default: ${AUTO_RECOVERY_PATH})
    -n --target-name       Target name, use with pg_create_restore_point()
    -t --target-tli        Target timeline to recovery to
    -T --target-time       Target time to recovery to (YYYYMMDDHHMMSS)
    -v --verbose           Verbose output
    -V --version           Show version and exit
    -x --target-xid        Target transaction ID to recovery to

EOF
    return 0
}

verbose() {
  ((VERBOSE >= 1)) && echo "$@"
}

debug() {
  ((DEBUG >= 1)) && echo "$@"
}

error() {
  echo "Error: $@" >&2
}

exit_with_error() {
  local code=$1; shift
  [[ $# -ge 1 ]] && error "$@"
  exit ${code}
}

show_help() {
    revision_details "${PROGRAM}" "${REVISION}"
    usage
cat <<DESC
    Recovery helper tool for pgbarman PITR
    Recover a postgresql PITR automatically or manually with a single command
DESC
    return 0
}

stop_postgres() {
  debug "Function: ${FUNCNAME}"
  if [[ ! -f "${RECOVERY_PATH}/postmaster.pid" ]]; then
    verbose "Unable to find PID file for PostgreSQL in ${RECOVERY_PATH}"
    return 1
  fi
  local PID=$(head -n1 "${RECOVERY_PATH}"/postmaster.pid)
  debug "PID: ${PID}"
  #kill -0 returns true if the process is found
  if kill -0 ${PID} &>/dev/null; then
    debug "PID: ${PID} found running"
    if pg_ctl -D "${RECOVERY_PATH}" -mfast stop &>/dev/null; then
      verbose "stopping PostgreSQL for pid: ${PID} with RECOVERY_PATH: ${RECOVERY_PATH}"
    else
      exit_with_error "${STATE_CRITICAL}" "RECOVERY_PATH: ${RECOVERY_PATH} could not be deleted"
    fi
  else
    verbose "No running PostgreSQL found for pid: ${PID}, continuing..."
  fi

}

kill_postgres() {
  debug "Function: ${FUNCNAME}"
  # found no pid file, attempt to kill PostgreSQL running with
  # current ${RECOVERY_PATH}, by getting PID, then sigTERM
  local PID=$(pgrep -u barman -f "postgres.+${RECOVERY_PATH}")
  if [[ ${PID} -gt 0 ]]; then
    verbose "PID of PostgreSQL to kill: ${GPID}"
    if kill -TERM "${PID}" &>/dev/null; then
      exit_with_error ${STATE_CRITICAL} "can't stop PostgreSQL with datadir "${RECOVERY_PATH}" and pid: ${PID}"
    else
      verbose "killed PostgreSQL with datadir "${RECOVERY_PATH}" and pid: ${PID}"
    fi
  else
    verbose "PostgreSQL is not running with datadir: ${RECOVERY_PATH}, continuing"
  fi
}

stop_recovery() {
  stop_postgres || kill_postgres
}

delete_recovery() {
  debug "Function: ${FUNCNAME}"
  if [[ -d ${RECOVERY_PATH} && ${RECOVERY_PATH} != / ]]; then
    if /bin/rm -rf "${RECOVERY_PATH}" &>/dev/null; then
      verbose "RECOVERY_PATH: ${RECOVERY_PATH} deleted"
    else
      exit_with_error ${STATE_CRITICAL} "RECOVERY_PATH: ${RECOVERY_PATH} could not be deleted"
    fi
  fi
}

list_backup() {
  debug "Function: ${FUNCNAME}"
  get_options
  barman list-backup --minimal "${RECOVERY_APPNAME}"
  local EXITCODE=$?
  if (( "${EXITCODE}" != 0 )) ; then
    exit_with_error ${STATE_CRITICAL} "barman list-backup failed with exit code: ${EXITCODE}"
  fi
}

get_latest_backup_name() {
  debug "Function: ${FUNCNAME}"
  # pick the top backup name
  LATEST_BACKUP=$(barman list-backup --minimal "${AUTO_RECOVERY_APPNAME}" |head -1)
  local EXITCODE=$?
  # exit immediately if we failed our test
  [[ "${EXITCODE}" -gt 0 ]] && exit_with_error "${EXITCODE}" \
    "Error, did not retrieve current dated (${DATE}) backup from barman"

  # if we're here, we're good to continue
  if [[ ${LATEST_BACKUP} =~ "${DATE}" ]]; then
    verbose "LATEST_BACKUP: ${LATEST_BACKUP}"
    verbose "AUTO_RECOVERY_APPNAME: ${AUTO_RECOVERY_APPNAME}"
    RECOVERY_BACKUP_NAME="${LATEST_BACKUP}"
  fi
}

start_barman_recovery() {
  debug "Function ${FUNCNAME}"
  if [[ ! -d "${RECOVERY_PATH}" ]]; then
    mkdir -p "${RECOVERY_PATH}"
  fi
  if [[ "${OPTIONS}" != "" ]]; then
    barman recover ${OPTIONS} > /tmp/pitr-recovery.log 2>&1
    local EXITCODE=$?
    if [[ ${EXITCODE} -gt 0 ]]; then
      exit_with_error "${EXITCODE}" "barman recover: ${OPTIONS}"
    fi
  else
    exit_with_error ${STATE_CRITICAL} "Error in OPTIONS value(S): ${OPTIONS}"
  fi
}

modify_recovery_config() {
  debug "Function: ${FUNCNAME}"
  local DATA_DIR="/var/lib/pgsql/9.3/data"
  local EXITCODE=$(sed -i -e "s/port = 5432/port=${RECOVERY_PORT}/" \
-e "s%data_directory = '${DATA_DIR}'%data_directory = '${RECOVERY_PATH}'%" \
${RECOVERY_PATH}/postgresql.conf)
  if [[ "${EXITCODE}" == 0 ]]; then
    exit_with_error ${STATE_CRITICAL} "failed to munge postgresql.conf \
with OPTIONS: ${RECOVERY_PATH} and ${RECOVERY_PORT}"
  fi
}

start_recovery_db() {
  debug "Function: ${FUNCNAME}"
  local EXITCODE=$(pg_ctl -D "${RECOVERY_PATH}" start &>/dev/null; echo -n $?)

  if [[ ${EXITCODE} -lt 1 ]]; then
    shopt -s nocaseglob
    case "${RECOVERY_MODE}" in
      manual)
      echo "PostgreSQL is running on port: ${RECOVERY_PORT}, in datadir: ${RECOVERY_PATH}"
      echo
      echo "stop the database (as barman) with 'pg_ctl -D "${RECOVERY_PATH}" -mfast stop'"
      ;;
      auto) verbose "PostgreSQL is recovered in automatic mode"
      ;;
      *) exit_with_error ${STATE_UNKNOWN} "unknown recovery_mode ${RECOVERY_MODE}"
      ;;
    esac
    shopt -u nocaseglob
  else
    exit_with_error ${STATE_CRITICAL} "PostgreSQL failed to start with exit code: ${EXITCODE}"
  fi
}

get_options() {
  debug "Function: ${FUNCNAME}"

  case "${RECOVERY_MODE}" in
    auto)
      RECOVERY_APPNAME=${AUTO_RECOVERY_APPNAME}
      RECOVERY_PATH=${AUTO_RECOVERY_PATH}
      RECOVERY_PORT=${AUTO_RECOVERY_PORT}
      OPTIONS="${RECOVERY_APPNAME} ${RECOVERY_BACKUP_NAME} ${RECOVERY_PATH}"
      ;;
    manual)
      RECOVERY_APPNAME="${APPNAME}"
      RECOVERY_BACKUP_NAME="${BACKUP_NAME}"
      RECOVERY_PATH="${MANUAL_RECOVERY_PATH}/${RECOVERY_BACKUP_NAME}"
      OPTIONS="${RECOVERY_APPNAME} ${RECOVERY_BACKUP_NAME} ${RECOVERY_PATH}"
      if [[ "${RECOVERY_TARGET_NAME}" != "" ]]; then
        OPTIONS="--target-name ${RECOVERY_TARGET_NAME} ${OPTIONS}"
      elif [[ "${RECOVERY_TARGET_TLI}" != "" ]]; then
        OPTIONS="--target-tli ${RECOVERY_TARGET_TLI} ${OPTIONS}"
      elif [[ "${RECOVERY_TARGET_TIME}" != "" ]]; then
        OPTIONS="--target-time ${RECOVERY_TARGET_TIME} ${OPTIONS}"
      elif [[ "${RECOVERY_TARGET_XID}" != "" ]]; then
        OPTIONS="--target-xid ${RECOVERY_TARGET_XID} ${OPTIONS}"
      fi
      ;;
    list)
      if [[ "${APPNAME}" != "" ]]; then
        RECOVERY_APPNAME=${APPNAME}
      else
        RECOVERY_APPNAME=${AUTO_RECOVERY_APPNAME}
      fi
      ;;
    *)
      exit_with_error ${STATE_CRITICAL} "Error: bad RECOVERY_MODE: ${RECOVERY_MODE}"
      ;;
  esac

  debug "RECOVERY_APPNAME:     ${RECOVERY_APPNAME}"
  debug "RECOVERY_BACKUP_NAME: ${RECOVERY_BACKUP_NAME}"
  debug "RECOVERY_PATH:        ${RECOVERY_PATH}"
  debug "OPTIONS:              ${OPTIONS}"
}

recover() {
  if [[ $# -ne 1 ]]; then
    exit_with_error ${STATE_UNKNOWN} "recover(): Expected 1 argument but found 0."
  elif [[ ! "$*" =~ (auto(mated)?|manual) ]]; then
    exit_with_error ${STATE_UNKNOWN} "recover(): Expected argument to be one of 'automated' or 'manual', but found '$*'"
  fi

  debug "Function: ${FUNCNAME}"
  [[ "$*" =~ auto(mated)? ]] && get_latest_backup_name
  get_options
  stop_recovery
  delete_recovery
  start_barman_recovery
  modify_recovery_config
  start_recovery_db
}

EXITCODE=${STATE_OK} #default
DEBUG=0
VERBOSE=0
[[ $# -lt 1 ]] && show_help && exit ${STATE_UNKNOWN}

OPTS=$(getopt --name "${PROGRAM}" -o p:a:n:t:T:x:b:vdrmplhV -l port:,appname,target-name,target-tli,target-time,target-xid,backup-name,verbose,debug,auto,manual,list-backup,help,version -- "$@")
[[ $? -ne 0 ]] && exit 1
eval set -- "${OPTS}"

while true; do
  case "$1" in
    --help|-h)         show_help; exit $STATE_OK;;
    --version|-V)      revision_details "${PROGRAM}" "${REVISION}"; exit $STATE_OK;;
    --verbose|-v)      VERBOSE="$((${VERBOSE} + 1))"; shift 1;;
    --debug|-d)        DEBUG="$((${DEBUG} + 1))"; shift 1;;
    --list-backup|-l)  RECOVERY_MODE=list; shift 1;;
    --auto|--automated|--robot|-r)
                       RECOVERY_MODE=auto; shift 1;;
    --manual|-m)       RECOVERY_MODE=manual; shift 1;;
    --port|-p)         RECOVERY_PORT=$2; shift 2;;
    --appname|-a)      APPNAME=$2; shift 2;;
    --target-name|-n)  RECOVERY_TARGET_NAME=$2; shift 2;;
    --target-tli|-t)   RECOVERY_TARGET_TLI=$2; shift 2;;
    --target-time|-T)  RECOVERY_TARGET_TIME=$2; shift 2;;
    --target-xid|-x)   RECOVERY_TARGET_XID=$2; shift 2;;
    --backup-name|-b)  BACKUP_NAME=$2; shift 2;;
    --) shift; break;;
    *) echo "Unknown argument: $1" && usage && exit $STATE_UNKNOWN;;
  esac
done

case "${RECOVERY_MODE}" in
  auto)   recover automated;;
  manual) recover manual;;
  list)   list_backup;;
esac

exit $EXITCODE
