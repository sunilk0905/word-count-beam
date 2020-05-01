def props = readJSON file: 'datatflow-config.json'
def mainClass = props['mainClass']
if(env.BRANCH_NAME == "master"){
    props = props['arguments']['prod']
}else if(env.BRANCH_NAME == "release"){
    props = props['arguments']['test']
}else{
    props = props['arguments']['dev']
}

def buildArguments(){
    def arguments = ""
    props.each { key, value ->
        echo "Walked through key $key and value $value"
        arguments += "--${key}=${value} "
    }
    new File("./target").eachFileMatch(~/.*bundled.*.jar/) { file ->
             arguments += "--filesToStage=" + file.getAbsolutePath()
             }
    return arguments;
}

node {
    stage("Pull Source Code"){
        git url: 'https://github.com/sunilk0905/word-count-beam.git'
    }
    stage("Package"){
            bat "mvn clean package -DskipTests=true -Pdataflow-runner"
    }
    stage("Create Dataflow Template"){
        withCredentials([file(credentialsId: 'gcp-service-account-key', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
              bat "echo %GOOGLE_APPLICATION_CREDENTIALS%"
              def arguments = buildArguments()
              bat 'mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=${mainClass} -Dexec.args="${arguments}"'
        }
    }
}