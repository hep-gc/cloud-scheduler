#!/bin/bash

cloud="mouse"
vm="testvm"
log="rest_client_test.log"
user="testuser"

status="./cloud_status $@"
admin="./cloud_admin $@"

tests=(
	"$status -h"
	"$status "                    # GET /cloud
	"$status -a"                  # GET /clusters
	"$status -a -j"               # GET /clusters.json
	"$status -b"                  # GET /vms?metric=startup_time
	"$status -c $cloud"           # GET /clusters/$cloud
	"$status -c $cloud -j"        # GET /clusters/$cloud.json
	"$status -c $cloud -n $vm"    # GET /clusters/$cloud/vms/$vm
	"$status -c $cloud -n $vm -j" # GET /clusters/$cloud/vms/$vm.json
	"$status -d"                  # GET /developer-info
	"$status -g"                  # GET /vms?metric=job_run_times
	"$status -i"                  # GET /ips
	"$status -l"                  # GET /cloud-config
	"$status -m"                  # GET /vms
	"$status -o"                  # GET /vms?metric=total
	"$status -o -c $cloud"        # GET /clusters/$cloud/vms?metric=total
	"$status -q all"              # GET /jobs?state=sched|new|high
	"$status -q complete"         # GET /jobs?state=complete
	"$status -q held"             # GET /jobs?state=held
	"$status -q high"             # GET /jobs?state=high
	"$status -q idle"             # GET /jobs?state=idle
	"$status -q new"              # GET /jobs?state=new
	"$status -q running"          # GET /jobs?state=running
	"$status -q sched"            # GET /jobs?state=sched
	"$status -r"                  # GET /clusters/$cloud/vms?metric=missing
	"$status -t"                  # GET /diff-types
	"$status -u"                  # GET /vms?metric=all
	"$status -u -c $cloud"        # GET /clusters/$cloud/vms?metric=all
	"$status -v"                  # GET /
	"$status -w"                  # GET /failures/image
	"$status -x"                  # GET /thread-heart-beats
	"$status -z"                  # GET /failures/boot

	"$admin"
	"$admin -h"
	"$admin -i"                           # POST /user-limits
	"$admin -j"                           # GET /user-limits
	"$admin -k -c $cloud -n $vm"          # PUT /clouds/$cloud/vms/$vm?action=shutdown
	"$admin -k -c $cloud -a"              # PUT /clouds/$cloud/vms?action=shutdown&count=all
	"$admin -k -c $cloud -b 1"            # PUT /clouds/$cloud/vms?action=shutdown&count=1
	"$admin -l DEBUG"                     # PUT /?log_level=DEBUG
	"$admin -m -c $cloud -n $vm"          # PUT /clouds/$cloud/vms/$vm?action=remove
	"$admin -m -c $cloud -a"              # PUT /clouds/$cloud/vms?action=remove&count=all
	"$admin -o -c $cloud -n $vm"          # PUT /clouds/$cloud/vms/$vm?action=force_retire
	"$admin -o -c $cloud -a"              # PUT /clouds/$cloud/vms?action=force_retire&count=all
	"$admin -o -c $cloud -b 1"            # PUT /clouds/$cloud/vms?action=force_retire&count=1
	"$admin -q"                           # POST /?action=quick_shutdown
	"$admin -t"                           # POST /cloud-aliases
	"$admin -u $user -p job"              # POST /users/$user?refresh=job_proxy
	"$admin -u $user -p vm"               # POST /users/$user?refresh=vm_proxy
	"$admin -v 5 -c $cloud"               # PUT /clouds/$cloud?allocations=5
	"$admin -y"                           # GET /cloud-aliases
	"$admin -x -c $cloud -n $vm"          # PUT /clouds/$cloud/vms/$vm?action=reset_override_state
	"$admin -d $cloud"                    # PUT /clouds/$cloud?action=disable
	"$admin -e $cloud"                    # PUT /clouds/$cloud?action=enable
)

> $log

test_total=${#tests[@]}
test_count=1

for test in "${tests[@]}"
do
	echo "[$test_count/$test_total] $test"
	echo -e "\n$test" >> $log

	$test >> $log

	((test_count++))
done