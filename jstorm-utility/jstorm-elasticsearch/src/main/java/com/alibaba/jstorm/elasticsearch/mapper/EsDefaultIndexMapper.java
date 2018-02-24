package com.alibaba.jstorm.elasticsearch.mapper;

import backtype.storm.tuple.ITuple;
import org.elasticsearch.action.DocWriteRequest;

public class EsDefaultIndexMapper implements EsIndexMapper<String> {

  private static final long serialVersionUID = 3777594656114668825L;

  @Override
  public DocWriteRequest.OpType getOpType() {
    return DocWriteRequest.OpType.INDEX;
  }

  @Override
  public String getIndex(ITuple tuple) {
    return tuple.getStringByField("index");
  }

  @Override
  public String getType(ITuple tuple) {
    return tuple.getStringByField("type");
  }

  @Override
  public String getId(ITuple tuple) {
    return tuple.getStringByField("id");
  }

  @Override
  public String getSource(ITuple tuple) {
    return tuple.getStringByField("source");
  }

}
