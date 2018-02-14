pipeline {
    agent none
    stages {
        stage('Test') {
            agent {
                docker {
                    image 'cloud:base'
                }
            }
            steps {
                sh 'python setup.py install'
                sh '''
                   cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler
                   cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler
                   '''
                sh '/etc/init.d/cloud_scheduler start'
            }
        }
    }
}
