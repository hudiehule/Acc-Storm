package org.apache.storm.starter;

import com.google.protobuf.ByteString;
import com.sun.org.apache.bcel.internal.util.ClassPath;
import org.apache.commons.exec.ExecuteException;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseAccBolt;
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
 * Created by Administrator on 2018/1/15.
 */
public class FStormTestTopology {

    public static class DataSpout extends BaseRichSpout {


        SpoutOutputCollector _collector;
        Random _rand;
        private static final String[] CHOICES = {
                "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers"
        };

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = ThreadLocalRandom.current();
        }

        @Override
        public void nextTuple() {
            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            Utils.sleep(1000);
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


    public static class MapBolt extends BaseAccBolt {

        public MapBolt(Class[] tupleEleTypes,int batchSize){
            super(tupleEleTypes,batchSize);
        }
        public void prepare(Map stormConf, TopologyContext context){
              System.out.println("Acc bolt preparation");
        }
        @Override
        public Class[] extractTupleElements(Tuple tuple){
             return new Class[1];
        }
        @Override
        public void execute(Tuple tuple,BasicOutputCollector collector){
         //用户执行逻辑 当这个组件不能在FPGA或者GPU上运行时 则还是放在CPU上运行  相应的在kernelFile中也有一种实现
            collector.emit(new Values("send"));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
           declarer.declare(new Fields("sentence"));
        }


    }

    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout",new DataSpout(),1);
        builder.setAccBolt("accbolt",new MapBolt(new Class[]{Integer.class},100)).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setNumWorkers(2);

        String kernel = "__kernel void "+
                        "multKernel(__global const float *a,"+
                        "             __global const float *b,"+
                        "             __global float *c)"+
                        "{"+
                        "    int gid = get_global_id(0);"+
                        "    c[gid] = a[gid] * b[gid];"+
                        "}";
        builder.setTopologyKernelFile(kernel, KernelFileArgumentType.FILESTRING);//设置kernel本地文件的路径
        String name = "FStormTestTopology"; //拓扑名称

        StormSubmitter.submitTopology(name,conf,builder.createTopology());

        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        Thread.sleep(60*1000); //运行十分钟
        //kill the topology
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

}
