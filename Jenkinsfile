pipeline {
    agent {
        kubernetes {
            cloud 'aks-corp-01'
            inheritFrom 'dind'
            namespace 'jenkins-agents'
        }
    }
    options {
        ansiColor('xterm')
    }

    parameters {
        booleanParam(name: 'RELOAD_SCM_ONLY', defaultValue: false, description: 'Check this to reload SCM configuration only and skip build/deploy')
        string(name: 'DEPLOYMENT_TIMEOUT', defaultValue: '240', description: 'Deployment timeout in seconds')
        choice(name: 'ENVIORMENT', choices: ['cldr-pl-us', 'ind'], description: 'Target environment')
        string(name: 'BASE_BRANCH', defaultValue: 'main', description: 'Branch to build from')
    }

    environment {
        B_ENV      = "${params.ENVIORMENT}"
        B_ID       = "LOCALAI_${params.ENVIORMENT}_${BUILD_NUMBER}"
        K8S_APP_NAME = "octopai-localai-${params.ENVIORMENT}"
    }

    stages {

        stage('Reload SCM Only') {
            when {
                expression { return params.RELOAD_SCM_ONLY == true }
            }
            steps {
                container('jnlp') {
                    echo 'Reloading SCM configuration only...'
                    checkout scm
                    echo 'SCM reloaded successfully. Skipping build and deploy stages.'
                }
                script {
                    currentBuild.result = 'SUCCESS'
                    currentBuild.description = 'SCM configuration reloaded'
                    return
                }
            }
        }

        stage('Preping ENV') {
            steps {
                script {
                    env.K8S_IMG_SECRET = 'acr-secret'

                    if (env.B_ENV == 'cldr-pl-us' || env.B_ENV == 'CLDR-PL-US') {
                        OCT_ACR          = 'cldracr'
                        ACR_CREDENTIALS  = 'octopai-cldrpl-us-acr-login'
                        K8S_ID           = 'cldr-pl-us-aks-config'
                        REPLICAS         = '1'
                        ENV_LOCATION     = 'us'
                    }

                    if (env.B_ENV == 'ind' || env.B_ENV == 'IND') {
                        OCT_ACR          = 'octindoacr'
                        ACR_CREDENTIALS  = 'octopai-prod-indonesia-acr'
                        K8S_ID           = 'config-prod-indonesia'
                        REPLICAS         = '1'
                        ENV_LOCATION     = 'ic'
                    }
                }
                sh 'printenv'
            }
        }

        stage('Build Images And Create The Tag') {
            when {
                expression { return params.RELOAD_SCM_ONLY == false }
            }
            steps {
                container('dind') {
                    sh 'ls'
                    echo 'Building LocalAI Docker image — model is baked into the image'
                    withCredentials([string(credentialsId: 'Jenkins-Git-Tokken2', variable: 'GIT_TOKEN')]) {
                        sh "docker image build -f Dockerfile -t localai:$B_ID ."
                        sh "docker image tag localai:$B_ID $OCT_ACR'.azurecr.io'/localai:$B_ID"
                    }
                }
            }
        }

        stage('Pushing Image') {
            when {
                expression { return params.RELOAD_SCM_ONLY == false }
            }
            steps {
                container('dind') {
                    echo 'Starting Push To ACR'
                    withCredentials([usernamePassword(credentialsId: "$ACR_CREDENTIALS", passwordVariable: 'ACR_PASSWD', usernameVariable: 'OCT_ACR_USR')]) {
                        sh "docker login $OCT_ACR'.azurecr.io' --username $OCT_ACR_USR --password $ACR_PASSWD"
                    }
                    sh "docker image push $OCT_ACR'.azurecr.io'/localai:$B_ID"
                }
            }
        }

        stage('Deploy App') {
            when {
                expression { return params.RELOAD_SCM_ONLY == false }
            }
            environment { ACR_IMG_URL = """$OCT_ACR"".azurecr.io""/localai:$B_ID""" }
            steps {
                container('az-runner') {
                    sh 'printenv'
                    sh "echo $ACR_IMG_URL"
                    sh """sed -i "s|K8S_APP_NAME|$K8S_APP_NAME|g" k8s.yaml"""
                    sh """sed -i "s|ACR_IMG_URL|$ACR_IMG_URL|g" k8s.yaml"""
                    sh """sed -i "s|K8S_APP_ENVIORMENT|$B_ENV|g" k8s.yaml"""
                    sh """sed -i "s|K8S_APP_LOCATION|$ENV_LOCATION|g" k8s.yaml"""
                    sh """sed -i "s|K8S_REPLICAS|$REPLICAS|g" k8s.yaml"""
                    sh 'cat k8s.yaml'
                    withKubeConfig([credentialsId: "${K8S_ID}"]) {
                        script {
                            // Create namespace if it doesn't exist
                            def nsExists = sh(script: "kubectl get ns ${env.K8S_APP_NAME}", returnStatus: true)
                            if (nsExists == 0) {
                                echo "Namespace '${env.K8S_APP_NAME}' already exists, skipping creation."
                            } else {
                                echo "Namespace '${env.K8S_APP_NAME}' does not exist, creating..."
                                sh "kubectl create ns ${env.K8S_APP_NAME}"
                            }
                        }
                        script {
                            // Create ACR image pull secret if it doesn't exist
                            def secretExists = sh(script: "kubectl get secret ${env.K8S_IMG_SECRET} --namespace ${env.K8S_APP_NAME}", returnStatus: true)
                            if (secretExists == 0) {
                                echo "Image pull secret '${env.K8S_IMG_SECRET}' already exists, skipping creation."
                            } else {
                                echo "Secret '${env.K8S_IMG_SECRET}' does not exist, creating..."
                                withCredentials([usernamePassword(credentialsId: "$ACR_CREDENTIALS", passwordVariable: 'ACR_PASSWD', usernameVariable: 'OCT_ACR_USR')]) {
                                    sh """
                                        kubectl create secret docker-registry ${env.K8S_IMG_SECRET} \
                                        --namespace ${env.K8S_APP_NAME} \
                                        --docker-server=${OCT_ACR}.azurecr.io \
                                        --docker-username=${OCT_ACR_USR} \
                                        --docker-password=${ACR_PASSWD}
                                    """
                                }
                            }
                        }
                        sh 'kubectl apply -f k8s.yaml'
                        echo "Waiting for deployment to be ready (timeout: ${params.DEPLOYMENT_TIMEOUT}s)..."
                        script {
                            def rolloutStatus = sh(script: "kubectl rollout status deployment/${env.K8S_APP_NAME} --namespace ${env.K8S_APP_NAME} --timeout=${params.DEPLOYMENT_TIMEOUT}s", returnStatus: true)
                            if (rolloutStatus != 0) {
                                echo 'Deployment failed or timed out. Rolling back to previous version...'
                                sh "kubectl rollout undo deployment/${env.K8S_APP_NAME} --namespace ${env.K8S_APP_NAME}"
                                sh "kubectl rollout status deployment/${env.K8S_APP_NAME} --namespace ${env.K8S_APP_NAME} --timeout=${params.DEPLOYMENT_TIMEOUT}s"
                                error('Deployment failed and was rolled back to previous version')
                            }
                            echo 'Deployment completed successfully!'
                        }
                    }
                }
            }
        }

        stage('Update changes') {
            when {
                expression { return params.RELOAD_SCM_ONLY == false }
            }
            steps {
                container('jnlp') {
                    checkout([$class: 'GitSCM',
                        branches: [[name: "*/${env.BASE_BRANCH}"]],
                        extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'source']],
                        userRemoteConfigs: [[credentialsId: 'gitgub_connect', url: 'git@github.com:OCTOPAILTD/octopai.localai.git']]
                    ])
                }
            }
        }
    }

    post {
        always {
            container('jnlp') {
                script {
                    currentBuild.description = "Built using <b>${env.BASE_BRANCH}</b> branch, <b>${env.B_ENV}</b> environment"
                }
            }
        }
    }
}
