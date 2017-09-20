package com.alibaba.jstorm.hdfs.bolt.parse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;

/**
 * 字节转换成avro
 * @author zhaoyaoyuan
 *
 */
public class BytesAvroParse implements TupleParse {
	private static final Logger LOG = LoggerFactory.getLogger(BytesAvroParse.class);
	
	/**
	 * String转换成Avro
	 * @throws IOException 
	 */
	@Override
	public GenericRecord tupleToRecode(Tuple tuple,Schema schema) throws IOException {
		GenericRecord avroRecord = new GenericData.Record(schema);
		List<Field> fieldList = schema.getFields();
		List<String> fields = new ArrayList<String>();
		for(Field field:fieldList) {
			fields.add(field.getProp("name"));
		}
		
		String messages = null;
		try {
			messages=new String((byte[]) ((TupleImplExt) tuple).get("bytes"),"UTF-8");
			JSONObject jsonObject = new JSONObject(messages);
			Iterator it = jsonObject.keys();
			while (it.hasNext()) {
				String key = (String) it.next();
				if(fields.contains(key)) {
					avroRecord.put(key, jsonObject.get(key));
					LOG.info("{}:{}",key,jsonObject.get(key));
				}else {
					LOG.debug("{} not in schema,{}", key,messages);
				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			LOG.error("字节转换String:{},{}", tuple.getValue(0), e);
			throw new IOException(e.getMessage());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			LOG.error("String转换成JSON{},{}", messages, e);
			throw new IOException(e.getMessage());
		}
		return avroRecord;
	}

}
