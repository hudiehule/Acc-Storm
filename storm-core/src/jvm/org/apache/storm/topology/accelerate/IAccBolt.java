package org.apache.storm.topology.accelerate;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IComponent;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Die_Hu on 2018/1/9.
 */
public interface IAccBolt extends Serializable{

    void accExecute(Tuple input);
    void accPrepare(Map stormConf, TopologyContext context,OutputCollector collector);
    void accCleanup();
}
