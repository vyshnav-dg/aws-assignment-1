pipeline {
    agent any

    environment {
        AWS_ACCESS_KEY_ID = credentials("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = credentials("AWS_SECRET_ACCESS_KEY")
        AWS_DEFAULT_REGION = "us-east-1"
    }

    stages {
        stage("Display AWS creds") {
            steps {
                echo "Using below AWS user"
                sh "aws sts get-caller-identity"
            }
        }
        dir("code") {
            stage("Build the project") {
                steps {
                    sh "sam build"
                }
            }
            stage("Deploy to AWS") {
                steps {
                    sh "sam deploy --no-confirm-changeset --no-fail-on-empty-changeset"
                }
            }
        }
    }
}