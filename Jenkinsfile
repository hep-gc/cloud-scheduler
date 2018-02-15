node{
    docker.image('cloud:base').inside{
        stage('Test'){
            sh 'systemctl start libvirtd'
            sh 'systemctl start condor'
            sh 'ls /var/log/condor'
            def condor = readFile('/var/log/condor/MasterLog')
            echo condor
        }
    }
}
