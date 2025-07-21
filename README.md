# test
for github test

mvn compile exec:java -D"exec.mainClass"="com.example.DataFlowDemo" -D"exec.args"="--runner=DirectRunner"

set GOOGLE_APPLICATION_CREDENTIALS=D:\workspace\git\dataflow-extract\myvpn-184506-8cc9909e08d5.json

mvn compile exec:java -D"exec.mainClass"="com.example.DataFlowDemo" -D"exec.args"="--runner=DataflowRunner --project=myvpn-184506 --gcpTempLocation=gs://dataflow-meta-bucket/temp --stagingLocation=gs://dataflow-meta-bucket/staging --region=us-central1 --jobName=dataflow-demo-job --autoscalingAlgorithm=THROUGHPUT_BASED --maxNumWorkers=5" -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7777 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7777


mvn compile exec:java -D"exec.mainClass"="com.example.GcsToGcs" -D"exec.args"="--runner=DirectRunner" -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7777 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7777

mvn clean compile exec:java -D"exec.mainClass"="com.example.GcsToGcsV2" -D"exec.args"="--runner=DirectRunner --pubSubSubscription=projects/myvpn-184506/subscriptions/my-gcs-notifications-subscription --destBucket=df-test-bucket-3" -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7777 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7777
