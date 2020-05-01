def buildArguments(props){
    def arguments = ""
    props.each { key, value ->
        arguments += "--${key}=${value} "
    }
    def workspace = pwd()
    new File("${workspace}/target").eachFileMatch(~/.*bundled.*\.jar/) { file ->
             arguments += "--filesToStage=" + file.getAbsolutePath()
             }
    return arguments;
}

node {

    def props = null
    def mainClass = null
    stage("Pull Source Code"){
        git url: 'https://github.com/sunilk0905/word-count-beam.git'
    }
    stage("Initialize Environment"){
        props = readJSON file: 'dataflow-config.json'
        mainClass = props['mainClass']
        if(env.BRANCH_NAME == "master"){
            props = props['arguments']['prod']
        }else if(env.BRANCH_NAME == "release"){
            props = props['arguments']['test']
        }else{
            props = props['arguments']['dev']
        }
    }
    stage("Package"){
            bat "mvn clean package -DskipTests=true -Pdataflow-runner"
    }
    stage("Create Dataflow Template"){
        withCredentials([file(credentialsId: 'gcp-service-account-key', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
              def arguments = buildArguments(props)
              bat "mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=${mainClass} -Dexec.args=\"${arguments}\""
        }
    }
}