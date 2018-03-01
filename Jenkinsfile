node{
    checkout scm

    docker.image('cloud-jenkins:conf').inside('--privileged'){
            stage('Test'){
                ///def hostip = sh(script: "ip -4 addr show eth0 | grep inet | awk '{print $2}' | awk -F '/' '{print $1}'", returnStdout: true).trim()
                ///echo hostip
                sh '''
                   HOSTIP = "ip -4 addr show eth0 | grep inet | awk '{print $2}' | awk -F '/' '{print $1}'"
                   sed -i "s/SETHOST/${HOSTIP}/g" /etc/condor/condor_config.local
                   sed -i "s/SETHOST/${HOSTIP}/g; s/#log_level: INFO/log_level: DEBUG/g" /etc/cloudscheduler/cloud_scheduler.conf
                   sed -i "s/myhost.localhost/'${HOSTIP}'/g" /etc/cloudscheduler/default.yaml
                   systemctl start libvirtd
                   systemctl start condor
                   systemctl start virtlogd

                   python setup.py install
                   cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler
                   cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler
                   /etc/init.d/cloud_scheduler start
                   '''
                sleep 10
                try{
                    sh '''
                       condor_q
                       cloud_status
                       virsh list --all
                       '''
                }
                catch(exc){
                    sh '''
                       cp /var/log/condor/MasterLog .
                       cp /tmp/cloud_scheduler.crash.log .
                       cp /var/log/cloudscheduler.log .
                       cp /etc/condor/condor_config.local .
                       cp /etc/cloudscheduler/cloud_scheduler.conf .
                       '''
                    archiveArtifacts artifacts: "cloudscheduler.log"
                    archiveArtifacts artifacts: 'MasterLog'
                    def crash = readFile "cloud_scheduler.crash.log"
                    echo crash
                    def condor_conf = readFile "condor_config.local"
                    echo condor_conf
                    def cloud_conf = readFile "cloud_scheduler.conf"
                    echo cloud_conf
                    error ('Something crashed...')
                    return
                }
 
                condor_nojob = sh( script: 'condor_q', returnStdout: true).trim()
                cloud_base = sh( script: 'cloud_status -m', returnStdout: true).trim()
                virsh_base = sh( script: 'virsh list --all', returnStdout: true).trim()

                try{
                    sh 'sudo -u hep condor_submit try.job'
                    sleep 15
                    sh '''
                       condor_q
                       cloud_status -m
                       virsh list --all
                       '''
                }
                catch(exc){
                    sh '''
                       cp /var/log/condor/MasterLog .
                       cp /tmp/cloud_scheduler.crash.log .
                       cp /var/log/cloudscheduler.log .
                       cp /var/log/condor/SchedLog .
                       cp /var/log/condor/NegotiatorLog .
                       '''
                    archiveArtifacts artifacts: "cloudscheduler.log"
                    archiveArtifacts artifacts: 'MasterLog'
                    archiveArtifacts artifacts: "SchedLog"
                    archiveArtifacts artifacts: "NegotiatorLog"
                    def crash = readFile "cloud_scheduler.crash.log"
                    echo crash
                    error ('Something crashed...')
                    return
                }
                
                condor_job = sh( script: 'condor_q', returnStdout: true).trim()
                cloud_check = sh( script: 'cloud_status -m', returnStdout: true).trim()
                sh '''
                   cp /etc/cloudscheduler/cloud_scheduler.conf .
                   cp /etc/condor/condor_config.local .
                   '''
                def condor_conf = readFile "condor_config.local"
                def cloud_conf = readFile "cloud_scheduler.conf"
                echo condor_conf 
                echo cloud_conf

                while (cloud_base == cloud_check){
                    echo cloud_base
                    echo cloud_check
                    sleep 10
                    cloud_check = sh( script: 'cloud_status -m', returnStdout: true).trim()
                }
                
                virsh_check = sh( script: 'virsh list --all', returnStdout: true).trim()
                if (virsh_base == virsh_check){
                    echo virsh_check
                    sh '''
                       cp /var/log/condor/MasterLog .
                       cp /tmp/cloud_scheduler.crash.log .
                       cp /var/log/cloudscheduler.log .
                       '''
                    archiveArtifacts artifacts: "cloudscheduler.log"
                    archiveArtifacts artifacts: 'MasterLog'
                    def crash = readFile "cloud_scheduler.crash.log"
                    echo crash
                    error("Problem with virsh...")
                    return
                }
                
                sh '''
                   cloud_status -m
                   virsh list --all
                   '''
                sleep 20
                condor_reg = sh( script: 'condor_status', returnStdout: true).trim()
                if (!condor_reg){
                  sleep 30
                  sh 'condor_status'  
                }
                sleep 20
                condor_reg = sh( script: 'condor_status', returnStdout: true).trim()
                if (!condor_reg){
                    sh '''
                       ls -lrt /tmp/tmp*
                       chmod 777 /tmp/tmp*/boot-log
                       cp /tmp/tmp*/boot-log .
                       condor_rm hep
                       cloud_admin -k -c container-cloud -a
                       '''
                    def boot = readFile "boot-log"
                    echo boot
                }

                else{
                    sleep 30
                    sh '''
                       condor_status
                       '''
                }
                sh '''
                   cp /var/log/cloudscheduler.log .
                   cp /tmp/cloud_scheduler.crash.log .
                   virsh list --all
                   cloud_status -m
                   '''
                def crash = readFile "cloud_scheduler.crash.log"
                echo crash
                archiveArtifacts artifacts: "cloudscheduler.log"
        }
    }
}
