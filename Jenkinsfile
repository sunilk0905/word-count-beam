node {
    stage("Pull Source Code"){
        git url: 'https://github.com/sunilk0905/word-count-beam.git'
    }
    stage("Create Dataflow Template"){
        withCredentials([file(credentialsId: 'gcp-service-account-key', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
              sh "echo %GOOGLE_APPLICATION_CREDENTIALS%"
              bat 'mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=org.apache.beam.examples.EmployeeAttendance -Dexec.args="--runner=DataflowRunner --project=research-poc-274116 --stagingLocation=gs://data_flow_bucket_borusu/staging --templateLocation=gs://data_flow_bucket_borusu/templates/employee-attendance --input=gs://data_flow_bucket_borusu/input/data_file.txt --output=gs://data_flow_bucket_borusu/output --tempLocation=gs://data_flow_bucket_borusu/temp --gcpTempLocation=gs://data_flow_bucket_borusu/temp"'
        }
    }
}