package com.alibaba.jstorm.hdfs.bolt.parse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;

/**
 * 字节转换成avro,从kafkatopic里取出来的是字节数组
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
		GenericRecord avroRecord = new GenericRecordBuilder(schema).build();

		String messages = null;
		try {
			messages=new String((byte[]) ((TupleImplExt) tuple).get("bytes"),"UTF-8");

			JSONObject jsonObject = JSONObject.parseObject(messages);
			for(Map.Entry<String,Object> entry:jsonObject.entrySet()){
				Boolean typeTrans=false;
				/**
				 *1、字段必须是 union类型，因为用的是getTypes方法
				 * 2、如果不是union类型，用getType方法
				 */
				for(Schema sa:schema.getField(entry.getKey()).schema().getTypes()){
					String type=sa.getType().getName();
					if("long".equalsIgnoreCase(type)){
						avroRecord.put(entry.getKey(),jsonObject.getLongValue(entry.getKey()));
						typeTrans=true;
						break;
					}else if("map".equalsIgnoreCase(type)){
						JSONObject mapObj = jsonObject.getJSONObject(entry.getKey());
						avroRecord.put(entry.getKey(),mapObj);
						typeTrans=true;
						break;
					}else if("boolean".equalsIgnoreCase(type)){
						avroRecord.put(entry.getKey(),jsonObject.getBoolean(entry.getKey()));
						typeTrans=true;
						break;
					}
				}
				if(!typeTrans){
					avroRecord.put(entry.getKey(),entry.getValue());
				}
			}
			//LOG.info("avro:{}",avroRecord);
//			while (it.hasNext()) {
//				String key = (String) it.next();
//				if(fields.contains(key)) {
//					avroRecord.put(key, jsonObject.get(key));
//					LOG.info("{}:{}",key,jsonObject.get(key));
//				}else {
//					LOG.debug("{} not in schema,{}", key,messages);
//				}
//			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("{},{}", messages, e);
			throw e;
		}
		return avroRecord;
	}
}
