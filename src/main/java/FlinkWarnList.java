import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class FlinkWarnList {

    public final static String[] IP = {"10.10.46.101", "10.10.46.102", "10.10.46.103"};

    public static List<String> markList = new ArrayList<String>();

    //public static Logger logger2 = LoggerFactory.getLogger(FlinkWarnList.class);

    public static Logger logger3 = LoggerFactory.getLogger(FlinkWarnList.class);

    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static List<AlarmRule> ruleList = new ArrayList<AlarmRule>();

    public static Date date1 = new Date();

    public static boolean flag = false;

    private static RestHighLevelClient client;


    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = PropUtil.loadProperties("/config.properties");
        Properties topicProp = PropUtil.loadProperties("/topic.properties");
        String topicString = topicProp.getProperty("topic");
        //logger2.info("topic=" + topicString);
        logger3.info("topic=" + topicString);

        client = EsUtil.getClient();
        EsUtil.createMarkIndex(client);

        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<String>(topicString, new SimpleStringSchema(), props));
        source.setParallelism(3);
        DataStream<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>> WarringMessage = source.flatMap(new FlatMapFunction<String, Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>>() {
            public void flatMap(String s, Collector<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>> collector) throws Exception {
                if (!flag) {
                    client = EsUtil.getClient();
                    ruleList = DBUtil.queryAlarmTable();
                    flag = true;
                }
                if (s != null && !"".equals(s)) {
                    logger3.debug("message=" + s);
                    String[] arrayString = StringUtils.splitByWholeSeparator(s, "(!@#$%)");
                    int length = arrayString.length;
                    Date date2 = new Date();
                    if (date2.getTime() - date1.getTime() > 1000 * 5) {
                        date1 = date2;
                        ruleList = DBUtil.queryAlarmTable();
                    }
                    if (length == 4) {
                        String ip = arrayString[0];
                        String filename = arrayString[1];
                        String message = arrayString[2];
                        //logger2.info("message=" + message);
                        long timestamp = Long.parseLong(arrayString[3]);
                        long rank = 1;
                        if (!markList.contains(ip + filename)) {
                            markList.add(ip + filename);
                            AllMark mark = new AllMark();
                            mark.setIp(ip);
                            mark.setRank(String.valueOf(rank));
                            mark.setFilename(filename);
                            IndexRequest indexRequest = new IndexRequest("mark", "user");
                            indexRequest.source(JSONObject.fromObject(mark), XContentType.JSON);
                            try {
                                client.index(indexRequest, EsUtil.defaultHeaders);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        boolean isWarn = true;
                        String deviceIp = arrayString[0];
                        for (AlarmRule alarmRule : ruleList) {
                            String keyword = alarmRule.getKeyword();
                            String fileNameRegex = alarmRule.getFilename();
                            if (message.indexOf(keyword) >= 0 && filename.matches(fileNameRegex)) {
                                isWarn = false;
                                String eventid = UUID.randomUUID().toString();
                                String eventtype = "LOG.APP";
                                String level = alarmRule.getLevel();
                                String content = message + alarmRule.getDescription();
                                Date date = new Date();
                                String time = format.format(date.getTime());
                                String processstatus = "0";
                                String eventstatus = "1";
                                String lasttime = time;
                                String eventtypeall = "LOG";
                                String isInsert = "0";
                                collector.collect(new Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>(eventid, eventtype
                                        , level, content, time, deviceIp, processstatus, eventstatus, lasttime, eventtypeall, isInsert, ip, filename, message, timestamp, rank));
                            }
                        }
                        if (isWarn) {
                            collector.collect(new Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>("", ""
                                    , "", "", "", "", "", "", "", "", "1", ip, filename, message, timestamp, rank));
                        }
                    }
                }
            }
        });

        WarringMessage.addSink(new MysqlSink());
        try {
            List<HttpHost> httpHosts = new ArrayList<HttpHost>();
            for (String ip : IP) {
                httpHosts.add(new HttpHost(ip, 9200, "http"));
            }

            ElasticsearchSink.Builder<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>>() {
                        public IndexRequest createIndexRequest(Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long> element) {
                            Template template = new Template();
                            template.setFilename(element.f12);
                            template.setMessage(element.f13);
                            template.setTime(element.f14);
                            template.setRank(element.f15);
                            String ipIndex = element.f11;
                            EsUtil.createIpIndexEs(ipIndex, client);
                            IndexRequest indexRequest = new IndexRequest(ipIndex, "user");
                            indexRequest.source(JSONObject.fromObject(template), XContentType.JSON);
                            return indexRequest;
                        }

                        public void process(Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                            indexer.add(createIndexRequest(element));
                        }
                    }
            );
            esSinkBuilder.setBulkFlushMaxActions(1);
            esSinkBuilder.setBulkFlushMaxSizeMb(500);
            //论缓冲操作的数量或大小如何都要刷新的时间间隔
            esSinkBuilder.setBulkFlushInterval(5000);
            WarringMessage.addSink(esSinkBuilder.build());
            env.execute("flink warn test");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
