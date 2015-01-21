#!/bin/bash
#
# Check whether servers booted on gridpp-imperial match between nova, cloud scheduler, and condor
# lists any inconsistencies.
#

E_BADARGS=85

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` openstack_rc_script [prefix]"
  exit $E_BADARGS
fi

# set up nova cli tools
source ${1}
COUNTER=0
NOVA_ID[0]=""

# read in prefix if one is given
PREFIX="server"
if [ -n "$2" ]
then
  PREFIX="$2"
fi

# loop over all servers in cloud interface
IFS=$'\n'
for NOVA_LINE in $(nova list | grep "^|" | grep -v ID | grep ${PREFIX});
do
    # get instance ID
    NOVA_ID[${COUNTER}]="$(echo ${NOVA_LINE} | awk '{print $2}')"

    # get instance name
    NOVA_NAME[${COUNTER}]="$(echo ${NOVA_LINE} | awk '{print $4}')"
    let COUNTER=COUNTER+1
done
IFS=$' '

# for each server found on nova
id=0
while [ ${id} -lt ${COUNTER} ]; do
  echo
  

  # check if the worker is registered in condor
  CONDOR_WORKER=$(condor_status -m | grep "${NOVA_NAME[${id}]}" | tr -d ' ')

  # check if cloud scheduler is tracking the server
  CS_VM=$(cloud_status -m | grep "${NOVA_NAME[${id}]}" | awk '{print $2}')

  # if cloud scheduler is tracking the VM, do nothing
  if [ -n "${CS_VM}" ]; then
    echo "Cloud Scheduler still has ${CS_VM}"
  else
    echo "Cloud Scheduler lost instance ${NOVA_NAME[${id}]}"

    # if the server is still on condor retire it
    if [ -n "${CONDOR_WORKER}" ]; then
      echo "   -> Retireing ${CONDOR_WORKER} from HTCondor"
      condor_off -peaceful -name "$CONDOR_WORKER" -startd
      # get startd CCB address:
      startd_addr=`condor_status ${CONDOR_WORKER} -l | grep MyAddress | head -1`
      startd_addr=${startd_addr#*\"}
      startd_addr=${startd_addr%\"}
      echo "      * retireing startd CCBID: ${startd_addr}"
      condor_off -peaceful -addr "${startd_addr}" -startd
      # get master CCB address:
      master_addr=`condor_status ${CONDOR_WORKER} -l -master | grep MasterIpAddr`
      master_addr=${master_addr#*\"}
      master_addr=${master_addr%\"}
      echo "      * retireing master CCBID: ${master_addr}"
      condor_off -peaceful -addr "${master_addr}" -master
    else
      # terminate instances that are neither on condor not on cloud scheduler
      echo "   -> Terminating Instance ${NOVA_NAME} ... "
      DELETE_IDS="${DELETE_IDS} ${NOVA_NAME[${id}]}"
    fi
  fi
  echo
  echo " ------------------------------- "
  let id=id+1
done
nova delete ${DELETE_IDS}

