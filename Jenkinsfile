pipeline {
    agent none
    stages {
        stage('Test') {
            agent {
                docker {
                    image 'cloud:base'
                    args '-v /home/:/home'
                }
            }
            steps {
                sh 'python setup.py install'
                sh '''
                   cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler
                   cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler
                   '''
                sh '/etc/init.d/cloud_scheduler start'
                sh 'cp /home/Documents/CentOS-7-x86_64-GenericCloud.img /jobs/instances/base'
            }
        }
    }
}
