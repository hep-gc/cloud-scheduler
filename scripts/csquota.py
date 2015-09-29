import sys
import json
import time
import urllib2
import subprocess

def main():
"""Alter path of cloud_* commands if needed and location of published quota info."""

    QUOTA_URL = "http://web.uvic.ca/~dleske/cql/cql.json"
    CLOUD_STATUS_CMD = "cloud_status -aj"
    CLOUD_ADMIN_CMD = "cloud_admin"
    

    args = CLOUD_STATUS_CMD.split()
    #args = ["/hepuser/mhp/workspace/cloudscheduler/cloud_status", "--config-file", "/hepuser/mhp/workspace/cloudscheduler/main.conf.bak", "-aj"]
    sp1 = subprocess.Popen(args, shell=False,
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = sp1.communicate(input=None)
    if out:
        try:
            resources = json.loads(out)
        except:
            print "Failed to parse json output from cloud status - is cloud_scheduler running?"
            sys.exit(1)
    else:
        print "failed to get json output from cloud_status, make sure cloud_status is in the path and cloud_scheduler is running."
        sys.exit(1)
    osn = []
    for res in resources['resources']:
        #print res
        if res['cloud_type'] == 'OpenStackNative':
            osn.append(res)
    #print osn[0]


    try:
        f = urllib2.urlopen(QUOTA_URL)
        data = f.read()
    except (urllib2.URLError,ValueError), e:
        print "Unable to read from url: %s" % QUOTA_URL
        data = None
        sys.exit(1)

    if data:
        quotas = json.loads(data)
        #print quotas, type(quotas)
    quota_adjustments = []
    for cloud in osn:
        #print cloud['name'], cloud['network_address']
        if quotas['source'] in cloud['network_address']: # match up the cloud with the quota message
            #print 'found the right cloud'
            #print cloud['name']
            # check if the tenant matches any of the ones for that cloud
            #print cloud['tenant']
            if cloud['tenant'] in quotas['quotas'].keys():
                #print 'found a matching tenant'
                #print 'value', quotas['quotas'][cloud['tenant']]
                quota_adjustments.append( (cloud['name'],quotas['quotas'][cloud['tenant']]))
            # save the cloud name and new tenant quota in a list to process the cloud_admin calls from
        # probably need some kind of loop for this not just an if statement for multiple sources? 
    # loop through the list to call cloud_admin and adjust quotas
    #print quota_adjustments
    args_adm = CLOUD_ADMIN_CMD.split()
    #args_adm = ["/hepuser/mhp/workspace/cloudscheduler/cloud_admin", "--config-file", "/hepuser/mhp/workspace/cloudscheduler/main.conf.bak"]
    for adj in quota_adjustments:
        adj_args = ['-c', adj[0], '-v', str(adj[1])]
        adj_cmd = args_adm + adj_args
        #print adj_cmd
        sp1 = subprocess.Popen(adj_cmd, shell=False,
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = sp1.communicate(input=None)
        time.sleep(1)

main()
