## 优化内容：
* 1、支持从tuple中的字节数组（BytesAvroParse）
* 2、支持目录日期变量（SimpleFileNameFormat）
* 3、支持hdfs 文件 append
## Package
```
    1、mvn clean package install -DskipTests
    2、mvn deploy:deploy-file -DgroupId=com.alibaba.jstorm -DartifactId=jstorm-hdfs -Dversion=2.4.0 -Dfile=target/jstorm-hdfs-2.4.0.jar -Durl=http://dev-bi-cdh07:8081/repository/bi-nexus/ -DrepositoryId=bi-nexus
```
## TODO
```dtd
    1、avro 不支持fields中的默认值 (GenericRecordBuilder可以创建有默认值的GenericRecord)
        avro中的字段类型必须是union类型的，default和第一个类型相匹配
    2、如果field是schema没有的，avro插入的时候报NullPointerException
    3、文件写入机制，只写入一个文件，这样可能会导致今天的数据写入到昨天的文件里。
```