## 优化内容：
* 1、支持从tuple中的字节数组（BytesAvroParse）
* 2、支持目录日期变量（SimpleFileNameFormat）
* 3、支持hdfs 文件续写
## Package
```
mvn deploy:deploy-file -DgroupId=com.alibaba.jstorm -DartifactId=jstorm-hdfs -Dversion=2.4.0 -Dfile=target/jstorm-hdfs-2.4.0.jar -Durl=http://dev-bi-cdh07:8081/repository/bi-nexus/ -DrepositoryId=bi-nexus
```
## TODO
```dtd
    1、avro 支持schem默认值
```
