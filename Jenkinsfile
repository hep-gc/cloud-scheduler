node{
    checkout scm

    docker.image('cloud-jenkins').withRun{ c->
        def ip =hostIp(c)

        docker.image('cloud-jenkins').inside(){
            stage('Test'){
                echo ip
            }
        }
    }
}

def hostIp(container) {
  sh "docker inspect -f {{.NetworkSettings.IPAddress}} ${container.id} > host.ip"
  readFile('host.ip').trim()
}
