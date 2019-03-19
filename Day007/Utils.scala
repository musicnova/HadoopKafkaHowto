package ru.croc.smartdata.spark;

import kafka.api.*;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class Utils {
    private final static org.apache.log4j.Logger log = org.apache.log4j.LogManager.getLogger(Utils.class);

    public final static String OFFSET_TABLE = "predictor_offsets";
    private final static byte[] OFFSET_CF = Bytes.toBytes("cf");

    private static Map<String, String> kafkaParams;

    private static final ThreadLocal<KafkaSender> kafkaSender = new ThreadLocal<>();
    private static final ThreadLocal<Configuration> hconf = new ThreadLocal<>();
    private static final ThreadLocal<Connection> hbase = new ThreadLocal<>();



    public static void setKafkaParams(Map<String, String> params) {
        kafkaParams = params;
    }

    public static Map<String, String> getKafkaParams() {
        return kafkaParams;
    }

    /**
     * Initialize and start Listener for control commands
     * @param port
     * @param ssc
     * @return
     */
    public static Thread initCommandListener(int port, JavaStreamingContext ssc) {
        Thread l = new Thread(new CommandListener() {
            @Override
            public void run() {
                try {
                    log.warn("Start listening for STOP command on " + port);
                    exploreSocketChannel(port);
                    log.warn("Received STOP command. Stopping context.");
                } catch (Exception e) {
                    log.error("Error listening for commands. Stopping context.", e);
                } finally {
                    ssc.stop(true, true);
                }
            }
        });
        l.start();
        return l;
    }

    /**
     * Load kafka-offsets stored in HBase
     * @param topic
     * @return
     */
    public static Map<TopicAndPartition, Long> loadOffsetFromHbase(String topic) {
        return loadOffsetFromHbase(topic, OFFSET_TABLE, OFFSET_CF);
    }

    /**
     * Load kafka-offsets stored in HBase
     * @param topic
     * @return
     */
    public static Map<TopicAndPartition, Long> loadOffsetFromHbase(String topic, String offsetTable) {
        return loadOffsetFromHbase(topic, offsetTable, OFFSET_CF);
    }

    /**
     * Load kafka-offsets stored in HBase
     * @param topic
     * @param offsetTable
     * @param offsetCf
     * @return Map
     */
    public static Map<TopicAndPartition, Long> loadOffsetFromHbase(String topic, String offsetTable, byte[] offsetCf) {
        Map<TopicAndPartition, Long> startOffsets = new HashMap<>();
        try(Connection hbase = getHbase(true); Table table = hbase.getTable(TableName.valueOf(offsetTable))) {
            Result r = table.get((new Get(Bytes.toBytes(topic))).setMaxVersions(1));
            try {
                NavigableMap<byte[], byte[]> cf = r.getFamilyMap(offsetCf);
                for (byte[] key : cf.keySet()) {
                    startOffsets.put(new TopicAndPartition(topic, Bytes.toInt(key)), Bytes.toLong(cf.get(key)));
                }
            } catch(NullPointerException e) {
                log.info("No offsets found for topic " + topic + ". Starting from 0L.");
                startOffsets.put(new TopicAndPartition(topic, 0), 0L);
            }
        } catch (IOException e) {
            log.error("Error loading offsets from HBase. Resetting to 0L.", e);
            startOffsets.put(new TopicAndPartition(topic, 0), 0L);
        }
        return startOffsets;
    }

    /**
     * Load offsets from Kafka for specified TopicAndPartition description
     * @param urlKafka
     * @param tap
     * @return Long
     */
    public static Long loadOffsetFromKafka(String urlKafka, TopicAndPartition tap) {
        /**
         * @TODO
         * - Split in two methods: first getting Seq<PartitionMetadata> from Kafka, second getting offset for PartitionMetadata
         * - In getStartOffsets() we should merge stored offsets with gotten here to automatically handling cases, when topic partitions change
         * - Optionally, get offsets for all partitions in one call
         */

        int soTimeout = 10000;
        int bufferSize = 100000;
        int maxWaitMs = 1000;
        int correlationId = 0;
        int numOffsets = 1;
        int time = -2; // -2 earliest, -1 latest
        String clientId = "";

        scala.collection.Seq<BrokerEndPoint> metadataTargetBrokers = ClientUtils.parseBrokerList(urlKafka);
        Set<String> topicSet = new HashSet<>();
        topicSet.add(tap.topic());
        scala.collection.Seq<TopicMetadata> topicsMetadata = ClientUtils.fetchTopicMetadata(scala.collection.JavaConverters.asScalaSetConverter(topicSet).asScala(), metadataTargetBrokers, clientId, maxWaitMs, correlationId).topicsMetadata();
        if(topicsMetadata.size() != 1 || !topicsMetadata.apply(0).topic().equals(tap.topic())) {
            log.error(String.format("Error: no valid topic metadata for topic: %s, probably the topic does not exist, run ", tap.topic()) + "kafka-list-topic.sh to verify");
            return -1L;
        }

        scala.collection.Iterator<PartitionMetadata> iter = topicsMetadata.head().partitionsMetadata().iterator();
        while(iter.hasNext()) {
            PartitionMetadata pm = iter.next();
            if (pm.partitionId() == tap.partition() && pm.leader().isDefined()) {
                BrokerEndPoint leader = pm.leader().get();
                SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), soTimeout, bufferSize, clientId);
                scala.collection.immutable.Map<TopicAndPartition, PartitionOffsetRequestInfo> map = new scala.collection.immutable.HashMap<>();
                map = map.$plus(new scala.Tuple2<>(tap, new PartitionOffsetRequestInfo(time, numOffsets)));
                OffsetRequest request = new OffsetRequest(map, OffsetRequest.CurrentVersion(), correlationId, clientId, -1);
                scala.collection.immutable.Map<TopicAndPartition, PartitionOffsetsResponse> resp = consumer.getOffsetsBefore(request).partitionErrorAndOffsets();
                if(resp.get(tap).isDefined()) {
                    return Long.valueOf(resp.get(tap).get().offsets().head().toString());
                } else {
                    log.warn("Received nothing for " + tap.toString());
                }
                consumer.close();
            } else {
                log.warn(String.format("Error: partition %s:%d does not have a leader. Skip getting offsets", tap.topic(), pm.partitionId()));
            }
        }
        return -1L;
    }

    /**
     * Load stored kafka-offsets from HBase validating them against real offsets loaded directly from Kafka
     * @param offsetTable
     * @param topic
     * @param kafkaParams
     * @return Map
     */
    public static Map<TopicAndPartition, Long> getStartOffsets(String topic, Map<String, String> kafkaParams, String offsetTable) {
        // get stored offsets
        Map<TopicAndPartition, Long> startOffsets = loadOffsetFromHbase(topic, offsetTable);
        //startOffsets.put(new TopicAndPartition(topic, 0), 0L);

        // get real offsets and validate stored against real
        Map<TopicAndPartition, Long> result = new HashMap<>();
        for(TopicAndPartition tap : startOffsets.keySet()) {
            long rOffset = loadOffsetFromKafka(kafkaParams.get("bootstrap.servers"), tap);
            if(rOffset >= 0L) {
                result.put(tap, Math.max(startOffsets.get(tap), rOffset));
            } else {
                log.warn("No offsets loaded for " + tap.toString() + ". Partition was skipped");
            }
        }
        log.info("Starting offsets loaded: " + result);
        return result;
    }

    /**
     * Store kafka-offsets in HBase
     * @param offsets
     */
    public static void updateStartOffsets(OffsetRange[] offsets) {
        updateStartOffsets(offsets, OFFSET_TABLE, OFFSET_CF);
    }

    /**
     * Store kafka-offsets in HBase
     * @param offsets
     * @param offsetTable
     */
    public static void updateStartOffsets(OffsetRange[] offsets, String offsetTable) {
        updateStartOffsets(offsets, offsetTable, OFFSET_CF);
    }

    /**
     * Store kafka-offsets in HBase
     * @param offsets
     * @param offsetTable
     * @param offsetCf
     */
    public static void updateStartOffsets(OffsetRange[] offsets, String offsetTable, byte[] offsetCf) {
        try(Connection hbase = getHbase(true)) {
            updateStartOffsets(hbase, offsets, offsetTable, offsetCf);
        } catch(IOException e) {
            log.error("Error saving offsets " + java.util.Arrays.toString(offsets), e);
            throw new RuntimeException(e);
        }
    }

    public static void updateStartOffsets(Connection hbase, OffsetRange[] offsets, String offsetTable, byte[] offsetCf) {
        try(Table table = hbase.getTable(TableName.valueOf(offsetTable))) {
            List<Put> puts = new ArrayList<>();
            for(OffsetRange o : offsets) {
                puts.add(new Put(Bytes.toBytes(o.topic())).addColumn(offsetCf, Bytes.toBytes(o.partition()), Bytes.toBytes(o.untilOffset())));
            }
            table.put(puts);
        } catch(IOException e) {
            log.error("Error saving offsets " + java.util.Arrays.toString(offsets), e);
            throw new RuntimeException(e);
        }
    }

    public static void closeResources() {
        try {
            KafkaSender l_kafkaSender = kafkaSender.get();
            if(l_kafkaSender != null) {
                l_kafkaSender.close();
                kafkaSender.remove();
            }
            Connection l_hbase = hbase.get();
            if(l_hbase != null) {
                if(!l_hbase.isClosed()) {
                    l_hbase.close();
                }
                hbase.remove();
            }
        } catch(Exception ex) {
            log.warn("Error on closing resources: ", ex);
        }
    }

    private static Configuration getHConf() {
        Configuration l_hconf = hconf.get();
        if(l_hconf == null) {
            l_hconf = HBaseConfiguration.create();
            hconf.set(l_hconf);
        }
        return l_hconf;
    }

    /**
     * Create new hbase connection
     * @return Connection
     * @throws IOException
     */
    public static Connection getHbase() throws IOException {
        Connection l_hbase = hbase.get();
        if(l_hbase == null || l_hbase.isClosed() || l_hbase.isAborted()) {
            l_hbase = ConnectionFactory.createConnection(getHConf());
            hbase.set(l_hbase);
        }
        return l_hbase;
    }

    public static Connection getHbase(boolean external) throws IOException {
        if(external) {
            return ConnectionFactory.createConnection(getHConf());
        }
        return getHbase();
    }

    /**
     * Execute a Put mutation to HBase
     * @param tableName
     * @param row
     * @param cf
     * @param qualifier
     * @param value
     */
    public static void putHbase(String tableName, byte[] row, byte[] cf, byte[] qualifier, byte[] value) {
        Put put = new Put(row).addColumn(cf, qualifier, value);
        putHbase(tableName, put);
    }

    /**
     * Execute a Put mutation to HBase
     * @param tableName
     * @param put
     */
    public static void putHbase(String tableName, Put put) {
        try {
            putHbase(getHbase(), tableName, put);
        } catch(IOException e) {
            log.error("Error on putHbase ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a Put mutation to HBase
     * @param hbase
     * @param tableName
     * @param put
     */
    public static void putHbase(Connection hbase, String tableName, Put put) {
        try(Table table = hbase.getTable(TableName.valueOf(tableName))) {
            table.put(put);
        } catch(IOException e) {
            log.error("Error on putHbase ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a Get query to HBase
     * @param tableName
     * @param row
     * @return Result
     */
    public static Result getHbase(String tableName, byte[] row) {
        return getHbase(tableName, new Get(row));
    }

    /**
     * Execute a Get query to HBase
     * @param tableName
     * @param get
     * @return Result
     */
    public static Result getHbase(String tableName, Get get) {
        try {
            return getHbase(getHbase(), tableName, get);
        } catch(IOException e) {
            log.error("Error on getHbase ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a Get query to HBase
     * @param hbase
     * @param tableName
     * @param get
     * @return Result
     */
    public static Result getHbase(Connection hbase, String tableName, Get get) {
        try(Table table = hbase.getTable(TableName.valueOf(tableName))) {
            return table.get(get);
        } catch(IOException e) {
            log.error("Error on getHbase ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a Delete mutation to HBase
     * @param tableName
     * @param row
     */
    public static void deleteHbase(String tableName, byte[] row) {
        try{
            deleteHbase(getHbase(), tableName, row);
        } catch(IOException e) {
            log.error("Error on getHbase ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a Delete mutation to HBase
     * @param hbase
     * @param tableName
     * @param row
     * @throws IOException
     */
    public static void deleteHbase(Connection hbase, String tableName, byte[] row) throws IOException {
        try (final Table table = hbase.getTable(TableName.valueOf(tableName))) {
            Delete del = new Delete(row);
            table.delete(del);
        }
    }

    /**
     * Execute a Scan query to HBase
     * @param hbase
     * @param tableName
     * @param scan
     * @return ResultScanner
     * @throws IOException
     */
    public static ResultScanner scanHbase(Connection hbase, String tableName, Scan scan) throws IOException {
        try (final Table table = hbase.getTable(TableName.valueOf(tableName)); final ResultScanner scanner = table.getScanner(scan);) {
            return scanner;
        }
    }

    /**
     * Execute a Scan query to HBase
     * @param tableName
     * @param scan
     * @return ResultScanner
     */
    public static ResultScanner scanHbase(String tableName, Scan scan) {
        try {
            return scanHbase(getHbase(), tableName, scan);
        } catch(IOException e) {
            log.error("Error on scanHbase ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Load script code as String from hdfs
     * @param path
     * @return String
     */
    public static String loadHdfsFile(String path) {
        Path filepath = new Path(path);
        try(FileSystem fs = FileSystem.newInstance(filepath.toUri(), new Configuration()); BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filepath)))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            log.error("Error loading script code from " + path, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Load script code as String from local path
     * @param path
     * @return String
     */
    public static String loadLocalFile(String path) {
        try {
            return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)));
        } catch(IOException e) {
            log.error("Error loading script code from " + path, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Load script by path and process it like it would be processed in Prediction Engine
     * For usage in testing or from console
     * @param scriptName
     * @param scriptParams
     * @return Integer
     * @throws Exception
     */
    public static int loadAndProcessScript(String scriptName, Object scriptParams) throws Exception {
        GroovyPredictor predictor = new GroovyPredictor(scriptName);
        return predictor.process(loadLocalFile(scriptName), scriptParams);
    }

    /**
     * Serialize Object to byte[]
     * @param obj
     * @return byte[]
     * @throws IOException
     */
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        out.flush();
        return bos.toByteArray();
    }

    /**
     * Enqueue predictor for calculation
     * @param name
     * @param params
     */
    public static void enqueuePredictor(String name, Object params) {
        sendToKafka(name, params);
    }

    private static KafkaSender getKafkaSender() {
        KafkaSender l_kafkaSender = kafkaSender.get();
        if(l_kafkaSender == null) {
            Properties props = new Properties();
            props.putAll(getKafkaParams());
            l_kafkaSender = new KafkaSender(props);
            kafkaSender.set(l_kafkaSender);
        }
        return l_kafkaSender;
    }

    /**
     * Send data as Object to Kafka topic
     * @param topic
     * @param params
     */
    public static void sendToKafka(String topic, Object params) {
        try {
            KafkaSender sender = getKafkaSender();
            if(params instanceof String || params instanceof groovy.lang.GString) {
                sender.send(topic, params.toString().getBytes(StandardCharsets.UTF_8));
            } else {
                sender.send(topic, serialize(params));
            }
        } catch(Exception ex) {
            log.error("Error adding predictor to Kafka", ex);
        }
    }

    /**
     * Calculate difference in days from today till specified date
     * @param date
     * @param format
     * @return
     */
    public static long calcTimeFromToday(byte[] date, String format, final java.time.temporal.ChronoUnit unit) {
        java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern(format);
        java.time.LocalDate endPeriod = java.time.LocalDate.parse(Bytes.toString(date), fmt);
        java.time.LocalDate today = java.time.LocalDate.now();
        return unit.between(today, endPeriod);
    }



    public static byte[] extractValue(Result row, byte[] cf, byte[] q) {
        List<Cell> list = row.getColumnCells(cf, q);
        return list == null || list.size() == 0 ? null : CellUtil.cloneValue(list.get(0));
    }

    public static Object callPredictor(String scriptPath, String scriptName, String scriptMethod, Object methodParams) throws Exception {
        String scriptCode = Utils.loadHdfsFile((scriptPath));
        GroovyPredictor predictor = new GroovyPredictor(scriptName);
        return predictor.call(scriptCode, scriptMethod, methodParams);
    }
}
