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
                sh '''
                   python setup.py install
                   cp scripts/cloud_scheduler.init.d /etc/init.d/cloud_scheduler
                   cp scripts/cloud_scheduler.sysconf /etc/sysconfig/cloud_scheduler

                   /etc/init.d/cloud_scheduler start
                   cat /tmp/cloud_scheduler.crash.log
                   '''
            }
        }
    }
}
