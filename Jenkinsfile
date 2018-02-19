node{
    checkout scm
    docker.image('cloud:base').inside('-v $WORKSPACE:/output'){
        stage('Test'){
            sh '''
               systemctl start libvirtd
               systemctl start condor
               python setup.py install
               cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler
               cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler
               /etc/init.d/cloud_scheduler start
               '''
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
                def cloud = readFile "cloudscheduler.log"
                echo cloud
                archiveArtifacts artifacts: 'MasterLog'
                def crash = readFile "cloud_scheduler.crash.log"
                echo crash
            }
        }
    }
}
