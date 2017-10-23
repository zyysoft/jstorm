package com.alibaba.jstorm.hdfs.bolt.parse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
		GenericRecord avroRecord = new GenericData.Record(schema);

		List<Field> fieldList = schema.getFields();
//		for(Field field:fieldList) {
//			avroRecord.put(field.name(),null);
//		}
		avroRecord.put("match",true);

		String messages = null;
		try {
			messages=new String((byte[]) ((TupleImplExt) tuple).get("bytes"),"UTF-8");

			JSONObject jsonObject = JSONObject.parseObject(messages);
			for(Map.Entry<String,Object> entry:jsonObject.entrySet()){
				avroRecord.put(entry.getKey(),entry.getValue());
				//解析数据类型
				for(Schema sa:schema.getField(entry.getKey()).schema().getTypes()){
					LOG.info("{}:{}",entry.getKey(),sa.getType().getName());
					String type=sa.getType().getName();
					if("long".equalsIgnoreCase(type)){
						avroRecord.put(entry.getKey(),jsonObject.getLongValue(entry.getKey()));
					}else if("map".equalsIgnoreCase(type)){
						JSONObject mapObj = jsonObject.getJSONObject(entry.getKey());
						avroRecord.put(entry.getKey(),mapObj);
					}
				}
			}
			LOG.info("tuple:{},avro:{}",jsonObject,avroRecord);
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
			LOG.error("{},{}", tuple.getValue(0), e);
			throw new IOException(e.getMessage());
		}
		return avroRecord;
	}
}
