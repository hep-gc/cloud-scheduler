pipeline {
    agent none
    stages {
        stage('Build') {
            agent {
                docker {
                    image 'cloud:base'
                }
            }
            steps {
                sh 'python setup.py install'
                sh 'cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler'
                sh 'cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler'
            }
        }
        stage('Test') {
            agent {
                docker {
                    image 'cloud:base'
                }
            }
            steps {
                 sh '/etc/init.d/cloud_scheduler start'
            }
        }
    }
}
