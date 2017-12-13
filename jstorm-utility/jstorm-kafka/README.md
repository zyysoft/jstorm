### 优化
---
* 1、支持kafka 0.11
* 2、Log.Debug数组下标越界
* 3、leader重新选举问题：当取到的leader去fetch时，leader发生了重新选举，导致该leader不可用
    fetch data from kafka topic[api_log] host[BI-HD12:9092] partition[4] error:6 message[This server is not the leader for that topic-partition.]
* 4、position失效问题，因为历史原因zk里有offset，然后topology重新跑时，这个offset已经删掉
    The requested offset is not within the range of offsets maintained by the server
* 5、如果一个消息在下游bolt处理失败，不会触发ack，即该offset不会从pendingOffsets删除，这样每次pendingOffsets.first()时，拿的都是
     同一个offset，导致offset不会写入到zk。而consumer还是在继续从kafka poll数据
* 6、修复pendingOffsets偶尔出现空指针问题
* 7、为什么会有offset没有ack的情况，好像不是Failed的问题。
* 8、为什么会有offset一会之前的，一会之后的？
* 9、kafka.fetch.from.beginning逻辑修改
### Build
---
```
1、 mvn deploy:deploy-file -DgroupId=com.alibaba.jstorm -DartifactId=jstorm-kafka -Dversion=2.2.1 -Dfile=target/jstorm-kafka-2.2.1.jar -Dsources=target/jstorm-kafka-2.2.1-sources.jar -Durl=http://dev-bi-cdh07:8081/repository/bi-nexus/ -DrepositoryId=bi-nexus
2、 mvn clean package install -DskipTests
```