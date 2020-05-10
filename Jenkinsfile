def buildArguments(envArgs, templateArgs){
    def arguments = ""
    envArgs.each { key, value ->
        arguments += "--${key}=${value} "
    }
    templateArgs.each { key, value ->
            arguments += "--${key}=${value} "
    }
    def workspace = pwd()
    new File("${workspace}/target").eachFileMatch(~/.*bundled.*\.jar/) { file ->
             arguments += "--filesToStage=" + file.getAbsolutePath()
             }
    return arguments;
}

node {
    def envProps = null
    stage("Pull Source Code"){
        git url: 'https://github.com/sunilk0905/word-count-beam.git'
    }
    stage("Initialize Environment"){
        envProps = readJSON file: 'dataflow-config.json'
    }
    stage("Package"){
            bat "mvn clean package -DskipTests=true -Pdataflow-runner"
    }
    stage("Create Dataflow Template"){
        def envArgs = null
        def templates = envProps['templates']
        if(env.BRANCH_NAME == "master"){
            envArgs = envProps['arguments']['prod']
        }else if(env.BRANCH_NAME == "release"){
            envArgs = envProps['arguments']['test']
        }else{
            envArgs = envProps['arguments']['dev']
        }
        withCredentials([file(credentialsId: 'gcp-service-account-key', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
            templates.each
            {
                template ->
                    {
                        def templateArgs = template['arguments']
                        def mainClass = template['mainClass']
                        def templateName = template['templateName']
                        echo "Creating template ${templateName}......"
                        def arguments = buildArguments(envArgs, templateArgs)
                        bat "mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=${mainClass} -Dexec.args=\"${arguments}\""
                    }
            }
        }
    }
}