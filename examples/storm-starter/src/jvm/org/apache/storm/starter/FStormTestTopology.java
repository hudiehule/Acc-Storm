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

import org.apache.storm.topology.*;
import org.apache.storm.topology.accelerate.BaseRichAccBolt;
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
 * Created by Administrator on 2018/1/15.
 */
public class FStormTestTopology {

    private static class SentWithTime {
        public final String sentence;
        public final long time;

        SentWithTime(String sentence, long time) {
            this.sentence = sentence;
            this.time = time;
        }
    }
    public static class C {
        LocalCluster _local = null;
        Nimbus.Client _client = null;

        public C(Map conf) {
            Map clusterConf = Utils.readStormConfig();
            if (conf != null) {
                clusterConf.putAll(conf);
            }
            Boolean isLocal = (Boolean)clusterConf.get("run.local");
            if (isLocal != null && isLocal) {
                _local = new LocalCluster();
            } else {
                _client = NimbusClient.getConfiguredClient(clusterConf).getClient();
            }
        }

        public ClusterSummary getClusterInfo() throws Exception {
            if (_local != null) {
                return _local.getClusterInfo();
            } else {
                return _client.getClusterInfo();
            }
        }

        public TopologyInfo getTopologyInfo(String id) throws Exception {
            if (_local != null) {
                return _local.getTopologyInfo(id);
            } else {
                return _client.getTopologyInfo(id);
            }
        }

        public void killTopologyWithOpts(String name, KillOptions opts) throws Exception {
            if (_local != null) {
                _local.killTopologyWithOpts(name, opts);
            } else {
                _client.killTopologyWithOpts(name, opts);
            }
        }

        public void submitTopology(String name, Map stormConf, StormTopology topology) throws Exception {
            if (_local != null) {
                _local.submitTopology(name, stormConf, topology);
            } else {
                StormSubmitter.submitTopology(name, stormConf, topology);
            }
        }

        public boolean isLocal() {
            return _local != null;
        }
    }
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
            _histo = new HistogramMetric(3600000000000L, 3);
            context.registerMetric("comp-lat-histo", _histo, 10); //Update every 10 seconds, so we are not too far behind
        }

        @Override
        public void nextTuple() {
            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                _collector.emit(new Values(sentence), new SentWithTime(sentence, _nextEmitTime - _periodNano));
                _emitsLeft--;
            }
        }
        @Override
        public void ack(Object id) {
            //Ignored
            long end = System.nanoTime();
            SentWithTime st = (SentWithTime)id;
            _histo.recordValue(end-st.time);
        }
        @Override
        public void fail(Object id) {
            SentWithTime st = (SentWithTime)id;
            _collector.emit(new Values(st.sentence), id);
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
    public static class MapBolt extends BaseRichAccBolt {
        private OutputCollector collector;
        public MapBolt(Class[] inputTupleEleTypes,Class[] outputTupleEleTypes,int batchSize,int tupleParallelism, String kernelName){
            super(inputTupleEleTypes,outputTupleEleTypes,batchSize,tupleParallelism,kernelName);
        }
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
            _collector.ack(tuple);
        }
        public void cleanup(){
            System.out.println("view bolt cleanup");
        }
    }

    private static class MemMeasure {
        private long _mem = 0;
        private long _time = 0;

        public synchronized void update(long mem) {
            _mem = mem;
            _time = System.currentTimeMillis();
        }

        public synchronized long get() {
            return isExpired() ? 0l : _mem;
        }

        public synchronized boolean isExpired() {
            return (System.currentTimeMillis() - _time) >= 20000;
        }
    }
    private static final Histogram _histo = new Histogram(3600000000000L, 3);
    private static final AtomicLong _systemCPU = new AtomicLong(0);
    private static final AtomicLong _userCPU = new AtomicLong(0);
    private static final AtomicLong _gcCount = new AtomicLong(0);
    private static final AtomicLong _gcMs = new AtomicLong(0);
    private static final ConcurrentHashMap<String, MemMeasure> _memoryBytes = new ConcurrentHashMap<String, MemMeasure>();
    private static long _prev_acked = 0;
    private static long _prev_uptime = 0;

    private static long readMemory() {
        long total = 0;
        for (MemMeasure mem: _memoryBytes.values()) {
            total += mem.get();
        }
        return total;
    }

    public static void printMetrics(C client, String name) throws Exception {
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
        for (ExecutorSummary exec: info.get_executors()) {
            if ("spout".equals(exec.get_component_id()) && exec.get_stats() != null && exec.get_stats().get_specific() != null) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                if (ackedMap != null) {
                    for (String key: ackedMap.keySet()) {
                        if (failedMap != null) {
                            Long tmp = failedMap.get(key);
                            if (tmp != null) {
                                failed += tmp;
                            }
                        }
                        long ackVal = ackedMap.get(key);
                        acked += ackVal;
                    }
                }
            }
        }
        long ackedThisTime = acked - _prev_acked;
        long thisTime = uptime - _prev_uptime;
        long nnpct, nnnpct, min, max;
        double mean, stddev;
        synchronized(_histo) {
            nnpct = _histo.getValueAtPercentile(99.0);
            nnnpct = _histo.getValueAtPercentile(99.9);
            min = _histo.getMinValue();
            max = _histo.getMaxValue();
            mean = _histo.getMean();
            stddev = _histo.getStdDeviation();
            _histo.reset();
        }
        long user = _userCPU.getAndSet(0);
        long sys = _systemCPU.getAndSet(0);
        long gc = _gcMs.getAndSet(0);
        double memMB = readMemory() / (1024.0 * 1024.0);
        System.out.printf("uptime: %,4d acked: %,9d acked/sec: %,10.2f failed: %,8d " +
                        "99%%: %,15d 99.9%%: %,15d min: %,15d max: %,15d mean: %,15.2f " +
                        "stddev: %,15.2f user: %,10d sys: %,10d gc: %,10d mem: %,10.2f\n",
                uptime, ackedThisTime, (((double)ackedThisTime)/thisTime), failed, nnpct, nnnpct,
                min, max, mean, stddev, user, sys, gc, memMB);
        _prev_uptime = uptime;
        _prev_acked = acked;
    }

    public static void kill(C client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception{
        if(args == null ||args.length <8){
            System.out.println("Please input paras: spoutNum bolt1Num bolt2Num numAckers numWorkers ratePerSecond batchSize isDebug");
        }else{
            int spoutNum = Integer.valueOf(args[0]);
            int bolt1Num = Integer.valueOf(args[1]);
            int bolt2Num = Integer.valueOf(args[2]);

            int numAckers = Integer.valueOf(args[3]);
            int numWorkers = Integer.valueOf(args[4]);

            int ratePerSecond = Integer.valueOf(args[5]);
            int batchSize = Integer.valueOf(args[6]);
            boolean isDebug = Boolean.valueOf(args[7]);

            Config conf = new Config();
            HttpForwardingMetricsServer metricServer = new HttpForwardingMetricsServer(conf) {
                @Override
                public void handle(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> dataPoints) {
                    String worker = taskInfo.srcWorkerHost + ":" + taskInfo.srcWorkerPort;
                    for (IMetricsConsumer.DataPoint dp: dataPoints) {
                        if ("comp-lat-histo".equals(dp.name) && dp.value instanceof Histogram) {
                            synchronized(_histo) {
                                _histo.add((Histogram)dp.value);
                            }
                        } else if ("CPU".equals(dp.name) && dp.value instanceof Map) {
                            Map<Object, Object> m = (Map<Object, Object>)dp.value;
                            Object sys = m.get("sys-ms");
                            if (sys instanceof Number) {
                                _systemCPU.getAndAdd(((Number)sys).longValue());
                            }
                            Object user = m.get("user-ms");
                            if (user instanceof Number) {
                                _userCPU.getAndAdd(((Number)user).longValue());
                            }
                        } else if (dp.name.startsWith("GC/") && dp.value instanceof Map) {
                            Map<Object, Object> m = (Map<Object, Object>)dp.value;
                            Object count = m.get("count");
                            if (count instanceof Number) {
                                _gcCount.getAndAdd(((Number)count).longValue());
                            }
                            Object time = m.get("timeMs");
                            if (time instanceof Number) {
                                _gcMs.getAndAdd(((Number)time).longValue());
                            }
                        } else if (dp.name.startsWith("memory/") && dp.value instanceof Map) {
                            Map<Object, Object> m = (Map<Object, Object>)dp.value;
                            Object val = m.get("usedBytes");
                            if (val instanceof Number) {
                                MemMeasure mm = _memoryBytes.get(worker);
                                if (mm == null) {
                                    mm = new MemMeasure();
                                    MemMeasure tmp = _memoryBytes.putIfAbsent(worker, mm);
                                    mm = tmp == null ? mm : tmp;
                                }
                                mm.update(((Number)val).longValue());
                            }
                        }
                    }
                }
            };
            metricServer.serve();
            String url = metricServer.getUrl();

            C cluster = new C(conf);
            conf.setNumWorkers(numWorkers);
            conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
            conf.registerMetricsConsumer(org.apache.storm.metric.HttpForwardingMetricsConsumer.class, url, 1);
            Map<String, String> workerMetrics = new HashMap<String, String>();
            if (!cluster.isLocal()) {
                //sigar uses JNI and does not work in local mode
                workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
            }
            conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
            conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);
            conf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
                    "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled");
            conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

            TopologyBuilder builder = new TopologyBuilder();


            builder.setSpout("spout",new DataSpout(ratePerSecond),spoutNum);
            builder.setBolt("split",new SplitBolt(),bolt1Num).shuffleGrouping("spout");
            builder.setAccBolt("compute",new MapBolt(new Class[]{char.class}, new Class[]{int.class},batchSize,1,"compute")).shuffleGrouping("split");
            builder.setBolt("view",new ViewBolt(),bolt2Num).shuffleGrouping("compute");

            conf.setNumWorkers(numWorkers);
            conf.setNumAckers(numAckers);
            conf.setDebug(isDebug);

            String aoclFileName = "compute";
            builder.setTopologyKernelFile(aoclFileName);//设置kernel本地可执行文件的路径 这个kernel必须是事先编译好的 提供kernel名称就可以了 去找
            String name = "FStormTestTopology"; //拓扑名称

            try {
                cluster.submitTopology(name, conf, builder.createTopology());

                for (int i = 0; i < 20 * 2; i++) {
                    Thread.sleep(30 * 1000);
                    printMetrics(cluster, name);
                }
            } finally {
                kill(cluster, name);
            }
            System.exit(0);
        }

    }

}
