node{
    checkout scm

    docker.image('cloud-jenkins:conf').inside('--privileged'){
            stage('Test'){
                sh '''
                   ifconfig
                   systemctl status libvirtd
                   systemctl status condor
                   systemctl status virtlogd
                   '''

        }
    }
}
