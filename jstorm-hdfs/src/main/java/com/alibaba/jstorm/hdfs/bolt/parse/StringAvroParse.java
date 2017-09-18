package com.alibaba.jstorm.hdfs.bolt.parse;

import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

public class StringAvroParse implements TupleParse {
	private static final Logger LOG = LoggerFactory.getLogger(StringAvroParse.class);
	
	/**
	 * String转换成Avro
	 */
	@Override
	public GenericRecord tupleToRecode(Tuple tuple,Schema schema) {
		GenericRecord avroRecord = new GenericData.Record(schema);
		try {
			JSONObject jsonObject = new JSONObject((String) tuple.getValue(0));
			Iterator it = jsonObject.keys();
			while (it.hasNext()) {
				String key = (String) it.next();
				avroRecord.put(key, jsonObject.get(key));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("字符串转换成JSON对象失败:{},{}", tuple.getValue(0), e);
			return null;
		}
		return avroRecord;
	}

}
