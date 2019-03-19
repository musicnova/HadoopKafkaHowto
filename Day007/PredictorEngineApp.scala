package ru.croc.smartdata.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class PredictorEngineApp implements Serializable {
    private final static org.apache.log4j.Logger log = org.apache.log4j.LogManager.getLogger(PredictorEngineApp.class);

    private final static String SCRIPT_PREFIX = "";
    private final static String SCRIPT_EXTENSION = ".groovy";

    private final static String CONF_SCRIPT_PATH_DEFAULT = "";

    @Parameter(names = "--help", help = true, description="Usage information")
    private boolean help = false;

    @Parameter(names={"--script-path", "-s"}, description="Directory containing predictor scripts")
    private String scriptPath = CONF_SCRIPT_PATH_DEFAULT;

    @Parameter(names={"--topics", "-t"}, required = true, splitter = com.beust.jcommander.converters.CommaParameterSplitter.class, description="Comma-separated list of topic names to stream from")
    private List<String> topics;

    @Parameter(names={"--broker", "-b"}, required = true, description="Kafka brokers url")
    private String urlKafka;

    @Parameter(names={"--interval", "-i"}, description="Streaming interval in miliseconds")
    private Long interval = 5000L;

    @Parameter(names={"--port", "-p"}, description="Port number for execution control")
    private Integer port = 9850;

    @Parameter(names={"--job-name", "-j"}, description="Spark job name")
    private String jobName = "PredictorEngine";

    @Parameter(names={"--offset-table", "-o"}, description="Name for the hbase table to use as offset storage")
    private String offsetTable = Utils.OFFSET_TABLE;

    @Parameter(names={"--offsetDistinct"}, description="Minimum number of records in topic to check duplicates")
    private Integer offsetDistinct = 500;

    @Parameter(names={"--offsetSplit"}, description="Minimum number of records in topic to split tasks")
    private Integer offsetSplit = 10;

    public static void main(String[] args) throws Exception {
        PredictorEngineApp app = new PredictorEngineApp();
        try {
            JCommander jc = new JCommander(app, args);
            if (app.help) {
                jc.usage();
                System.exit(0);
            }
        } catch(ParameterException ex) {
            System.out.println(ex.getMessage());
            System.exit(0);
        }
        app.execute();
    }

    protected void execute() throws Exception {

        // init spark config
        SparkConf conf = new SparkConf().setAppName(jobName);
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.streaming.concurrentJobs", String.valueOf(topics.size()));

        SparkContext sc = new SparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(new JavaSparkContext(sc), Durations.milliseconds(interval));

        // init kafka params
        Map<String, String> kafkaParams = getKafkaParams();

        // schedule each topic in separate thread
        /** @TODO or should all topics be scheduled as one stream? */
        boolean s = false;
        for (String topic : topics) {
            if(!s) {
                processTopic(ssc, kafkaParams, topic);
                s = true;
            } else {
                (new Thread(() -> {
                    try {
                        processTopic(ssc, kafkaParams, topic);
                    } catch (Exception e) {
                        log.error("Error initialising stream for topic '" + topic + "'", e);
                    }
                })).start();
            }
        }

        /** @TODO Included STOP-command listener from kakadu. Anyhow avoid code duplication? */
        Utils.initCommandListener(port, ssc);

        Thread.sleep(30000L);
        if(ssc.getState() != StreamingContextState.STOPPED) {
            ssc.start();
            ssc.awaitTermination();
        }
    }

    private void processTopic(JavaStreamingContext ssc, Map<String, String> kafkaParams, String topic) throws Exception {
        final String offsetTable = this.offsetTable;
        Map<TopicAndPartition, Long> startOffsets = Utils.getStartOffsets(topic, kafkaParams, offsetTable);

        // use pool to divide
        ssc.sparkContext().setLocalProperty("spark.scheduler.pool", "pool-" + topic);

        // create stream
        JavaInputDStream<Object> stream = KafkaUtils.createDirectStream(
                ssc,
                String.class, Object.class, StringDecoder.class, ObjectDecoder.class, Object.class,
                kafkaParams, startOffsets, (Function<MessageAndMetadata<String, Object>, Object>) MessageAndMetadata::message);

        final AtomicReference<Map<TopicAndPartition, Long>> prevOffsets = new AtomicReference<>();
        prevOffsets.set(startOffsets);
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        final Integer offsetDistinct = this.offsetDistinct;
        final Integer offsetSplit = this.offsetSplit;

        stream
            // getting current offsets must be first operation
            .transform(
                    (Function<JavaRDD<Object>, JavaRDD<Object>>) rdd -> {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
            )
            // apply predictor
            .foreachRDD((VoidFunction2<JavaRDD<Object>, Time>) (rdd, time) -> {
                long offsetMaxInPartition = 0L;
                long offsetCount = 0L;
                for (OffsetRange r : offsetRanges.get()) {
                    long offsetPartition = r.untilOffset() - prevOffsets.get().getOrDefault(r.topicAndPartition(), 0L);
                    offsetMaxInPartition = Math.max(offsetMaxInPartition, offsetPartition);
                    offsetCount += offsetPartition;
                }
                if(offsetCount > 0L) {
                    if(offsetCount > offsetDistinct) {
                        rdd = rdd.distinct();
                    }
                    rdd = offsetMaxInPartition > offsetSplit ? rdd.repartition((int)Math.ceil(offsetCount / offsetSplit)) : rdd;
                    rdd.foreachPartition(iter -> {
                        if (iter.hasNext()) {
                            Utils.setKafkaParams(kafkaParams);
                            String scriptName = getScriptName(topic);
                            GroovyPredictor predictor = new GroovyPredictor(scriptName);
                            try {
                                String scriptCode = loadScript(scriptName);
                                while (iter.hasNext()) {
                                    Object data = iter.next();
                                    try {
                                        int result = predictor.process(scriptCode, data);
                                        if (result != GroovyPredictor.SCRIPT_SUCCESS) {
                                            log.warn(String.format("Predictor %s for topic %s returned code %s (should be %s for success)", scriptName, topic, result, GroovyPredictor.SCRIPT_SUCCESS));
                                        }
                                    } catch (Exception ex) {
                                        log.error("data: " + (data != null ? data.getClass() + " - " + data.toString() : "null"));
                                        log.error(String.format("Error executing predictor %s for topic %s : %s", scriptName, topic, ex.getMessage()), ex);
                                    }
                                }
                            } catch (Exception ex) {
                                log.error(String.format("Error loading predictor %s for topic %s : %s", scriptName, topic, ex.getMessage()), ex);
                            }
                        }
                        Utils.closeResources();
                    });
                    Utils.updateStartOffsets(offsetRanges.get(), offsetTable);
                    for (OffsetRange r : offsetRanges.get()) {
                        prevOffsets.get().put(r.topicAndPartition(), r.untilOffset());
                    }
                }
            });
    }

    private String loadScript(String scriptName) {
        return Utils.loadHdfsFile((scriptPath.charAt(scriptPath.length()-1) != File.separatorChar ? scriptPath + File.separatorChar : scriptPath) + scriptName);
    }

    private String getScriptName(String topic) {
        //topic = topic.substring(0, 1).toUpperCase() + topic.substring(1)
        return SCRIPT_PREFIX + topic + SCRIPT_EXTENSION;
    }

    private Map<String, String> getKafkaParams() {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", urlKafka);
        kafkaParams.put("group.id", "predictor-engine");
        kafkaParams.put("auto.offset.reset", "smallest"); // for v0.10 use earliest
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("fetch.message.max.bytes", "10485760"); // 10*1024*1024 bytes
        return kafkaParams;
    }
}
