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
               cp /var/log/condor/MasterLog .
               cp /tmp/cloud_scheduler.crash.log .
               '''
            archiveArtifacts artifacts: 'MasterLog'
            archiveArtifacts artifacts: 'cloud_scheduler.crash.log'
        }
    }
}
