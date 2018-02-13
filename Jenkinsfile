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
            }
        }
    }
}
