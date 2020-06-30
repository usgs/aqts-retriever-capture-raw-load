@Library(value='iow-ecs-pipeline@2.2.0', changelog=false) _

pipeline {
    agent {
        node {
            label 'project:any'
        }
    }
    parameters {
        choice(choices: ['TEST', 'QA', 'PROD-EXTERNAL'], description: 'Deploy Stage (i.e. tier)', name: 'DEPLOY_STAGE')
    }
    triggers {
        pollSCM('H/5 * * * *')
    }
    stages {
        stage('run build the zip file for lambda') {
            agent {
                dockerfile true
            }
            steps {
                sh '''
                npm install
                ./node_modules/serverless/bin/serverless deploy --stage ${DEPLOY_STAGE} --region us-west-2
                '''
            }
        }
    }
    post {
        always {
            script {
                pipelineUtils.cleanWorkspace()
            }
        }
    }
}
