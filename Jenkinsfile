node{
    checkout scm
    docker.image('cloud:base').inside('-v $WORKSPACE:/output'){
        stage('Test'){
            sh 'systemctl start libvirtd'
            sh 'systemctl start condor'
            sh '''
               cp /var/log/condor/MasterLog .
               ls
               '''
        }
    }
}
