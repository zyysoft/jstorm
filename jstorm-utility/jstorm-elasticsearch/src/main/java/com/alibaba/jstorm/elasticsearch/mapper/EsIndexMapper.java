package com.alibaba.jstorm.elasticsearch.mapper;

import backtype.storm.tuple.ITuple;
import org.elasticsearch.action.DocWriteRequest;

public interface EsIndexMapper<T> extends EsMapper {

  public DocWriteRequest.OpType getOpType();

  public String getId(ITuple tuple);

  public T getSource(ITuple tuple);

}
