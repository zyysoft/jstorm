package com.alibaba.jstorm.hdfs.bolt.parse;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import backtype.storm.tuple.Tuple;

public class DefaultAvroParse implements TupleParse{

	@Override
	public GenericRecord tupleToRecode(Tuple tuple,Schema schema) {
		// TODO Auto-generated method stub
		return (GenericRecord) tuple.getValue(0);
	}
}
