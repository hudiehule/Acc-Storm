package org.apache.storm.starter;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/11/27.
 */
public class GrepOnStorm {
    private static final Logger LOG = Logger.getLogger(GrepOnStorm.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String FM_ID = "find";
    public static final String CM_ID = "count";

    public static class SentenceGeneratorSpout extends BaseRichSpout {
        static final String[] SENTENCES = new String[]{ "the cow  hustr jumped over the moon substring ", "an apple a day keeps subvalue the doctor away hudie hule ",
                "four score and subtitle seven years ago", "snow white and subject the seven dwarfs", "i am at subsection two with nature" };

        SpoutOutputCollector _collector;
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;

        public SentenceGeneratorSpout(long ratePerSecond) {
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
                String sentence = SENTENCES[_rand.nextInt(SENTENCES.length)];
                _collector.emit(new Values(sentence), sentence);
                _emitsLeft--;
            }
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("str"));
        }
    }

    public static class SplitSentence extends BaseBasicBolt {

        public static final String FIELDS = "word";

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getStringByField("str");
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word.toCharArray()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }

    }
    public static class FindMatchingWord extends BaseBasicBolt {
        public static final String FIELDS = "matching";
        private Pattern pattern;
        private Matcher matcher;
        private final String ptnString;

        public FindMatchingWord(String ptnString) {
            this.ptnString = ptnString;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            pattern = Pattern.compile(ptnString);
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = new String((char[])input.getValue(0));
            LOG.debug(String.format("find pattern %s in sentence %s", ptnString, sentence));
            matcher = pattern.matcher(sentence);
            if (matcher.find()) {
                collector.emit(new Values(1));
            }else{
                collector.emit(new Values(0));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class CountMatchingWord extends BaseBasicBolt {
        public static final String FIELDS = "count";
        private int count = 0;

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            if (input.getInteger(0).equals(1)) {
                collector.emit(new Values(count++));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    private static long _prev_acked = 0;
    private static long _prev_uptime = 0;
    private static double _prev_weightedAvgTotal = 0;

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
            if (SPOUT_ID.equals(exec.get_component_id())) {
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
        long ackedThisTime = acked - _prev_acked;
        long thisTime = uptime - _prev_uptime;
        double weightedAvgTotalThisTime = weightedAvgTotal - _prev_weightedAvgTotal;
        double avgLatencyThisTime = weightedAvgTotalThisTime/ackedThisTime;
        System.out.println("uptime: "+uptime + "-" + _prev_uptime +" ackedThisTime: "+ackedThisTime+" avgLatency: "+avgLatencyThisTime+" acked/sec: "+(((double)ackedThisTime)/thisTime+" failed: "+failed));
        _prev_uptime = uptime;
        _prev_acked = acked;
        _prev_weightedAvgTotal = weightedAvgTotal;
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }


    public static void main(String[] args) throws Exception{
        if(args == null ||args.length < 10){
            System.out.println("Please input paras: spoutNum splitNum matchNum countNum numAckers numWorkers ratePerSecond isDebug matchStr isKafka");
        }else {
            int spoutNum = Integer.valueOf(args[0]);
            int splitNum = Integer.valueOf(args[1]);
            int matchNum = Integer.valueOf(args[2]);

            int countNum = Integer.valueOf(args[3]);
            int numAckers = Integer.valueOf(args[4]);
            int numWorkers = Integer.valueOf(args[5]);

            int ratePerSecond = Integer.valueOf(args[6]);
            boolean isDebug = Boolean.valueOf(args[7]);

            String patterStr = args[8];
            boolean isKafkaSpout = Boolean.valueOf(args[9]);
            Config conf = new Config();

            TopologyBuilder builder = new TopologyBuilder();
            if(isKafkaSpout){
                SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("xeon+fpga:2181,xeon+fpga:2182,dell:2181"),
                        "datasource","/ostormdata", UUID.randomUUID().toString());
                spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
                builder.setSpout(SPOUT_ID,kafkaSpout,spoutNum);
            }else{
                builder.setSpout(SPOUT_ID,new SentenceGeneratorSpout(ratePerSecond),spoutNum);
            }
            builder.setBolt(SPLIT_ID, new SplitSentence(),splitNum).shuffleGrouping(SPOUT_ID);
            builder.setBolt(FM_ID, new FindMatchingWord(patterStr),matchNum).shuffleGrouping(SPLIT_ID);
            builder.setBolt(CM_ID, new CountMatchingWord(),countNum).fieldsGrouping(FM_ID, new Fields(FindMatchingWord.FIELDS));

            conf.setDebug(isDebug);
            conf.setNumAckers(numAckers);
            conf.setNumWorkers(numWorkers);

            String name = "GrepOnStorm"; //拓扑名称

            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());


            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(Utils.readCommandLineOpts());
            Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

            Thread.sleep(1000 * 90);
            for (int i = 0; i < 30; i++) {
                Thread.sleep(30 * 1000);
                printMetrics(client, name);
            }
            kill(client, name);

        }
    }
}
