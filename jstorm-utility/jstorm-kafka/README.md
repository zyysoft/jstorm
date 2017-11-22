### 优化
---
* 1、支持kafka 0.11
* 2、Log.Debug数组下标越界
* 3、leader重新选举问题：当取到的leader去fetch时，leader发生了重新选举，导致该leader不可用
    fetch data from kafka topic[api_log] host[BI-HD12:9092] partition[4] error:6 message[This server is not the leader for that topic-partition.]
* 4、position失效问题，因为历史原因zk里有offset，然后topology重新跑时，这个offset已经删掉
    The requested offset is not within the range of offsets maintained by the server
### Build
---
```
1、 mvn deploy:deploy-file -DgroupId=com.alibaba.jstorm -DartifactId=jstorm-kafka -Dversion=2.2.1 -Dfile=target/jstorm-kafka-2.2.1.jar -Dsources=target/jstorm-kafka-2.2.1-sources.jar -Durl=http://dev-bi-cdh07:8081/repository/bi-nexus/ -DrepositoryId=bi-nexus
2、 mvn clean package -DskipTests
```