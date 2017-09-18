package com.alibaba.jstorm.hdfs.bolt.parse;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import backtype.storm.tuple.Tuple;

/**
 * tuple解析成avro recode
 * @author zhaoyaoyuan
 *
 */
public interface TupleParse extends Serializable{
	 GenericRecord tupleToRecode(Tuple tuple,Schema schema) throws IOException;
}