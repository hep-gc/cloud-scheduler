node{
    checkout scm
    docker.image('cloud:base').inside('-v /hepuser/tahyaw/Documents:/home'){
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
               ls -l /home/containers/update-test
               '''
            try{
                sh '''
                   ls
                   sudo -u hep condor_submit /home/containers/update-test/try.job
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
