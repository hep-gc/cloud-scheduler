node{
    checkout scm
    docker.image('cloud-jenkins:logs').inside('-v /hepuser/tahyaw/Documents:/user'){
        stage('Test'){
            sh '''
               systemctl start libvirtd
               systemctl start condor
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
                return
            }
            sh '''
               ls -l /home
               '''
            try{
                sh '''
                   sudo -u hep condor_submit try.job
                   condor_q
                   cloud_status -m
                   '''
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
                return
            }
        }
    }
}
