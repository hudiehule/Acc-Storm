package org.apache.storm.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.accelerate.IRichAccBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by Administrator on 2018/1/15.
 */
public class AccBoltExecutor implements IRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(BasicBoltExecutor.class);

    private IRichAccBolt _bolt;
    private transient BasicOutputCollector _collector;

    public AccBoltExecutor(IRichAccBolt bolt) {
        _bolt = bolt;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
    }


    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _bolt.prepare(stormConf, context,collector);
        _collector = new BasicOutputCollector(collector);
    }

    public void execute(Tuple input) {
        _collector.setContext(input);
        try {
            _bolt.execute(input);
            _collector.getOutputter().ack(input);
        } catch(FailedException e) {
            if(e instanceof ReportedFailedException) {
                _collector.reportError(e);
            }
            _collector.getOutputter().fail(input);
        }
    }


    public void cleanup() {
        _bolt.cleanup();
    }

    public Map<String, Object> getComponentConfiguration() {
        return _bolt.getComponentConfiguration();
    }

}
