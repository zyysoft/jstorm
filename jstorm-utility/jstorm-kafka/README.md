### 优化
* 1、支持kafka 0.11
* 2、Log.Debug数组下标越界
* 3、leader重新选举问题：当取到的leader去fetch时，leader发送了重新选举，导致该leader不可用
### Build
```
1、 mvn deploy:deploy-file -DgroupId=com.alibaba.jstorm -DartifactId=jstorm-kafka -Dversion=2.2.1 -Dfile=target/jstorm-kafka-2.2.1.jar -Dsources=target/jstorm-kafka-2.2.1-sources.jar -Durl=http://dev-bi-cdh07:8081/repository/bi-nexus/ -DrepositoryId=bi-nexus
2、 mvn clean package -DskipTests
```