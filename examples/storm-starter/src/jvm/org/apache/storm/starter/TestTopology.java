package org.apache.storm.starter;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
                "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers"
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
    }

    public static class SplitBolt extends BaseRichBolt {

        OutputCollector _collector;
        private TestThread testThread;
        class TestThread extends Thread{
            private volatile boolean cancel = false;
            @Override
            public void run() {
                while(!cancel){
                    Utils.sleep(1000);
                    System.out.println("test Thread");
                }
            }
            public void shutdown(){
                cancel = true;
            }
        }
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
            testThread = new TestThread();
            testThread.start();
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
            System.out.println(testThread.getState());
            _collector.ack(tuple);
        }

        public void cleanup(){
            System.out.println("waiting for result thread state: "+testThread.getState());
            System.out.println("waiting for result thread is alive: "+testThread.isAlive());
            System.out.println("waiting for result thread is interrupted: "+testThread.isInterrupted());
            System.out.println("thread stack element:");
            for(StackTraceElement ele : testThread.getStackTrace()){
                System.out.println(ele.toString());
            }
            testThread.shutdown();
            System.out.println("bolt cleanup");
        }
    }

    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source",new DataSpout(1000),1);
        builder.setBolt("bolt",new SplitBolt(),1).shuffleGrouping("source");

        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        StormSubmitter.submitTopology("test",conf,builder.createTopology());
    }
}
