### 优化
* 1、支持kafka 0.11

```
mvn deploy:deploy-file -DgroupId=org.apache.kafka -DartifactId=kafka_2.11 -Dversion=0.11.0.0 -Dfile=target/jstorm-kafka-2.2.1.jar -Durl=http://dev-bi-cdh07:8081/repository/bi-nexus/ -DrepositoryId=bi-nexus
```