node{
    checkout scm

    docker.image('cloud-jenkins:conf').withRun{ c->
        def ip =hostIp(c)

        docker.image('cloud-jenkins:conf').inside('--privileged'){
            stage('Test'){
                sh '''
                   sed -i "s/SETHOST/${ip}/g" /etc/condor/condor_config.local
                   sed -i "s/SETHOST/${ip}/g" /etc/cloud_scheduler/cloud_scheduler.conf
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
                       '''
                    archiveArtifacts artifacts: "cloudscheduler.log"
                    archiveArtifacts artifacts: 'MasterLog'
                    def crash = readFile "cloud_scheduler.crash.log"
                    echo crash
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
                       '''
                    archiveArtifacts artifacts: "cloudscheduler.log"
                    archiveArtifacts artifacts: 'MasterLog'
                    def crash = readFile "cloud_scheduler.crash.log"
                    echo crash
                    error ('Something crashed...')
                    return
                }
                
                condor_job = sh( script: 'condor_q', returnStdout: true).trim()
                cloud_check = sh( script: 'cloud_status -m', returnStdout: true).trim()
                virsh_check = sh( script: 'virsh list --all', returnStdout: true).trim()

                while (cloud_base == cloud_check){
                    sleep 10
                    cloud_check = sh( script: 'cloud_status -m', returnStdout: true).trim()
                }
                if (virsh_base == virsh_check){
                    echo virsh_base
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
                
                condor_reg = sh( script: 'condor_status', returnStdout: true).trim()
                
                if (condor_reg){
                    echo 'Nothing yet!'
                }
                
                sh '''
                   condor_rm hep
                   cloud_admin -k -c container-cloud -a'
                   '''
            }
        }
    }
}

def hostIp(container) {
  sh "docker inspect -f {{.NetworkSettings.IPAddress}} ${container.id} > host.ip"
  readFile('host.ip').trim()
}
