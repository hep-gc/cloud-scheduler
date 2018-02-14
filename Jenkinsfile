pipeline {
    agent {
        docker {
            image 'cloud:base'
        }
    }
    stages {
        stage('Build') {
            steps {
                sh 'python setup.py install'
                sh 'cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler'
                sh 'cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler'
            }
        }
        stage('Test') {
            steps {
                 sh '/etc/init.d/cloud_scheduler start'
            }
        }
    }
}
