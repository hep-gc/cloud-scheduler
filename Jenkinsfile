node{
    docker.image('cloud:base').inside{
        stage('Test'){
            sh 'systemctl start libvirtd'
            sh 'systemctl start condor'
            sh 'condor_q'
        }
    }
}
