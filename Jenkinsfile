#!groovy

milestone 0
timestamps {
    node('docker') {
        checkout scm

        docker.withRegistry('https://harbor.cyverse.org', 'jenkins-harbor-credentials') {
            def dockerImage
            stage('Build') {
                milestone 50
                dockerImage = docker.build("harbor.cyverse.org/de/database-merger:${env.BUILD_TAG}")
                milestone 51
            }
            stage('Docker Push') {
                milestone 100
                dockerImage.push()
                dockerImage.push("${env.BRANCH_NAME}")
                // retag to 'qa' if this is the main branch
                if ( "${env.BRANCH_NAME}" == "main" ) {
                    dockerImage.push("qa")
                }
                milestone 101
            }
        }
    }
}
