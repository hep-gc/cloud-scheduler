node{
    checkout scm
    docker.image('cloud-jenkins').inside('--privileged'){
        stage('Test'){
            sh '''
               cp qemu.conf /etc/libvirt/
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
                   virsh net-list --all
                   sudo -u hep condor_submit try.job
                   condor_q
                   cloud_status -q all
                   '''
                sleep 15
                sh '''
                   condor_q
                   cloud_status -m
                   virsh list --all
                   '''
                sleep 15
                sh '''
                   condor_q 
                   cloud_status -m
                   cloud_status -q all
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
            sh '''
               cp /var/log/condor/MasterLog .
               cp /tmp/cloud_scheduler.crash.log .
               cp /var/log/cloudscheduler.log .
               '''
            archiveArtifacts artifacts: "cloudscheduler.log"
            archiveArtifacts artifacts: 'MasterLog'
            def crash = readFile "cloud_scheduler.crash.log"
            echo crash
        }
    }
}
