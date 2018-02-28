package com.alibaba.jstorm.elasticsearch.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.alibaba.jstorm.elasticsearch.mapper.EsIndexMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EsIndexBolt extends EsAbstractBolt {
  private  Logger LOG = LoggerFactory.getLogger(EsIndexBolt.class);

  private static final long serialVersionUID = 8177473361305606986L;

  private EsIndexMapper mapper;

  public EsIndexBolt(EsConfig esConfig, EsIndexMapper mapper) {
    super(esConfig);
    this.mapper = mapper;
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      if(!TupleHelpers.isTickTuple(tuple)) {
          String index = mapper.getIndex(tuple);
          String type = mapper.getType(tuple);
          String id = mapper.getId(tuple);
          if(mapper.getSource(tuple) instanceof Map){
              Map source = (Map)mapper.getSource(tuple);
              String opt = (String) source.get("Elastic_OpType");
              source.remove("Elastic_OpType");
              if("DELETE".equalsIgnoreCase(opt)){
                  client.prepareDelete(index, type,id).execute().get();
              }else {
                  IndexRequest indexReq = new IndexRequest(index,type,id)
                          .source(source);
                  UpdateRequest updateReq = new UpdateRequest(index, type, id)
                          .doc(source)
                          .upsert(indexReq);
                  client.update(updateReq).get();
              }
          }
      }
      collector.ack(tuple);
    } catch (Exception e) {
        LOG.error("{}",e);
      collector.fail(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
