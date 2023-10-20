jobName("Publish")
jobDescription("Publish google-cloud-patch artifacts")

properties([
    pipelineTriggers([githubPush()]),
])

node("microservice-builder") {
    timeout(time: 10, unit: "MINUTES") {
        stride("Checkout") {
            cleanWs()
            checkout scm
        }

        stride("Deploy") {
            dir("gcp-cloud-sql-serverless-patch") {
                withServiceAccount("service-account-java-publish") {
                    sh """
                      mvn pl.project13.maven:git-commit-id-plugin:revision versions:set
                      mvn deploy --batch-mode
                    """
                }
            }
        }
    }

    failure("Notification") {
        slackSend color: "bad", message: "Jenkins Job failed: ${jobName()}\n${BUILD_URL}"
    }
}

def withServiceAccount(String credentialsId, Closure body) {
    withCredentials([file(credentialsId: credentialsId, variable: "GOOGLE_APPLICATION_CREDENTIALS")]) {
        sh "gcloud auth activate-service-account --key-file=\${GOOGLE_APPLICATION_CREDENTIALS}"

        try {
            body()
        } finally {
            sh "gcloud auth revoke \$(gcloud config get-value account)"
        }
    }
}
