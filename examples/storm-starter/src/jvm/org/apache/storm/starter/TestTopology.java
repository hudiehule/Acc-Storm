package org.apache.storm.starter;


import org.HdrHistogram.Histogram;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.metric.HttpForwardingMetricsServer;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metrics.hdrhistogram.HistogramMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2018/9/10.
 */
public class TestTopology {



    public static class DataSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;
        HistogramMetric _histo;
        private static final String[] CHOICES = {
                "marry had a little lamb whos fleese was white as snow marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go marry had a little lamb whos fleese was white as snow",
                "one two three four five six seven eight nine ten one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers peter piper picked a peck of pickeled peppers peter piper picked a peck of pickeled peppers"
        };
        public DataSpout(long ratePerSecond){
            if (ratePerSecond > 0) {
                _periodNano = Math.max(1, 1000000000/ratePerSecond);
                _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
            } else {
                _periodNano = Long.MAX_VALUE - 1;
                _emitAmount = 1;
            }
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = ThreadLocalRandom.current();
            _nextEmitTime = System.nanoTime();
            _emitsLeft = _emitAmount;
        }

        @Override
        public void nextTuple() {
            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                _collector.emit(new Values(sentence), sentence);
                _emitsLeft--;
            }
        }
        @Override
        public void ack(Object id) {
            //Ignored

        }
        @Override
        public void fail(Object id) {

            _collector.emit(new Values(id), id);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

        public void close(){
            System.out.println("data source close()");
        }
    }


    public static class SplitBolt extends BaseRichBolt{
        OutputCollector _collector;
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("char element"));
        }
        public void execute(Tuple tuple){
            String sentence = tuple.getString(0);
            char[] ch = sentence.toCharArray();
            for (char ele : ch) {
                if(ele >= 'a' && ele <= 'z' || ele >= 'A' && ele <= 'Z') _collector.emit(tuple,new Values(ele));
            }
            _collector.ack(tuple);
        }

        public void cleanup(){
            System.out.println("split bolt cleanup()");
        }
    }
    public static class MapBolt extends BaseRichBolt {
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context,OutputCollector collector){
            this.collector = collector;
        }
        @Override
        public void execute(Tuple tuple){
            //用户执行逻辑 当这个组件不能在FPGA或者GPU上运行时 则还是放在CPU上运行  相应的在kernelFile中也有一种实现
            char a = (char)tuple.getValue(0);
            int value;
            if(a > 'a' && a< 'z'){
                value = a - 'a';
            }else{
                value = a - 'A';
            }
            collector.emit(tuple,new Values(value * value));
            collector.ack(tuple);
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("char_int_value"));
        }

        public void cleanup(){
            System.out.println("map compute cleanup()");
        }
    }

    public static class ViewBolt extends BaseRichBolt{
        OutputCollector _collector;
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
        }
        public void execute(Tuple tuple){
            int value = tuple.getInteger(0);
            System.out.println("result: " + value);
            _collector.ack(tuple);
        }
        public void cleanup(){
            System.out.println("view bolt cleanup");
        }
    }

    public static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec: info.get_executors()) {
            if ("spout".equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key: ackedMap.keySet()) {
                    if (failedMap != null) {
                        Long tmp = failedMap.get(key);
                        if (tmp != null) {
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal += latVal;
                }
            }
        }
        double avgLatency = weightedAvgTotal/acked;
        System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed));
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception{
        if(args == null ||args.length <8){
            System.out.println("Please input paras: spoutNum bolt1Num bolt2Num numAckers numWorkers ratePerSecond mapBoltNum isDebug");
        }else{
            int spoutNum = Integer.valueOf(args[0]);
            int bolt1Num = Integer.valueOf(args[1]);
            int bolt2Num = Integer.valueOf(args[2]);

            int numAckers = Integer.valueOf(args[3]);
            int numWorkers = Integer.valueOf(args[4]);

            int ratePersecond = Integer.valueOf(args[5]);
            int mapBoltNum = Integer.valueOf(args[6]);
            boolean isDebug = Boolean.valueOf(args[7]);

            Config conf = new Config();
            conf.setNumWorkers(numWorkers);
            conf.setNumAckers(numAckers);
            conf.setDebug(isDebug);
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("spout",new DataSpout(ratePersecond),spoutNum);
            builder.setBolt("split",new SplitBolt(),bolt1Num).shuffleGrouping("spout");
            builder.setBolt("compute",new MapBolt(),mapBoltNum).shuffleGrouping("split");
            builder.setBolt("view",new ViewBolt(),bolt2Num).shuffleGrouping("compute");
            String name = "TestTopology"; //拓扑名称

            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());


            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(Utils.readCommandLineOpts());
            Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

            for (int i = 0; i < 20; i++) {
                Thread.sleep(30 * 1000);
                printMetrics(client, name);
            }
            kill(client, name);
        }

    }
}
