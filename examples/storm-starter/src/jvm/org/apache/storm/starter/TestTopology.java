package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Administrator on 2018/9/10.
 */
public class TestTopology {
    public static class DataSpout extends BaseRichSpout {
        private int sleepTime;
        SpoutOutputCollector _collector;
        Random _rand;
        private static final String[] CHOICES = {
                "marry had a little lamb whos fleese was white as snow marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go marry had a little lamb whos fleese was white as snow",
                "one two three four five six seven eight nine ten one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers peter piper picked a peck of pickeled peppers peter piper picked a peck of pickeled peppers"
        };
        public DataSpout(int time){
            this.sleepTime = time;
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = ThreadLocalRandom.current();
        }

        @Override
        public void nextTuple() {
            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            Utils.sleep(sleepTime);
            _collector.emit(new Values(sentence), sentence);
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
                if(ele != ' ') _collector.emit(tuple,new Values(ele));
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
            System.out.println("Acc bolt preparation");
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
            int new_value = value * 2;
            _collector.ack(tuple);
        }
        public void cleanup(){
            System.out.println("view bolt cleanup");
        }
    }
    public static void main(String[] args) throws Exception{
        if(args == null ||args.length <8){
            System.out.println("Please input paras: spoutNum bolt1Num bolt2Num numAckers numWorkers sleepTime batchSize");
        }else{
            int spoutNum = Integer.valueOf(args[0]);
            int bolt1Num = Integer.valueOf(args[1]);
            int bolt2Num = Integer.valueOf(args[2]);

            int numAckers = Integer.valueOf(args[3]);
            int numWorkers = Integer.valueOf(args[4]);

            int sleepTime = Integer.valueOf(args[5]);
            int mapBoltNum = Integer.valueOf(args[6]);
            boolean isDebug = Boolean.valueOf(args[7]);
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("spout",new DataSpout(sleepTime),spoutNum);
            builder.setBolt("split",new SplitBolt(),bolt1Num).shuffleGrouping("spout");
            builder.setBolt("compute",new MapBolt(),mapBoltNum).shuffleGrouping("split");
            builder.setBolt("view",new ViewBolt(),bolt2Num).shuffleGrouping("compute");

            Config conf = new Config();
            conf.setNumWorkers(numWorkers);
            conf.setNumAckers(numAckers);
            conf.setDebug(isDebug);

            String name = "TestTopology"; //拓扑名称

            StormSubmitter.submitTopology(name,conf,builder.createTopology());

            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(Utils.readCommandLineOpts());
            Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

            Thread.sleep(600*500); //运行十分钟
            //kill the topology
            KillOptions opts = new KillOptions();
            opts.set_wait_secs(0);
            client.killTopologyWithOpts(name, opts);
        }

    }
}
