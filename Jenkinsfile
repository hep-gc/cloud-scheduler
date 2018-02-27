node{
    stage 'start database'
    
    docker.image('redis:3.0.7-alpine').withRun { c ->
        def ip = hostIp(c)
        
        stage 'client set'
        
        docker.image('redis:3.0.7-alpine').inside {
            sh "redis-cli -h ${ip} set test 123"
        }
        
        stage 'client get'
        
        docker.image('redis:3.0.7-alpine').inside {
            sh "redis-cli -h ${ip} get test"
        }
        
        stage 'client del'
        
        docker.image('redis:3.0.7-alpine').inside {
            sh "redis-cli -h ${ip} del test"
        }
    }
}

def hostIp(container) {
  sh "docker inspect -f {{.NetworkSettings.IPAddress}} ${container.id} > host.ip"
  readFile('host.ip').trim()
}
