import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class FlinkWarnByKey {

    public static Logger logger2 = LoggerFactory.getLogger(FlinkWarnByKey.class);

    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static List<AlarmRule> ruleList = new ArrayList<AlarmRule>();

    public static Date date1 = new Date();

    public static boolean flag = false;


    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = PropUtil.loadProperties("/config.properties");
        Properties topicProp = PropUtil.loadProperties("/topic.properties");
        String topicString = topicProp.getProperty("topic");
        logger2.info("topic=" + topicString);

        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<String>(topicString, new SimpleStringSchema(), props));
        source.setParallelism(3);
        DataStream<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> WarringMessage = source.map(new MapFunction<String, Tuple11<String, String, String, String, String, String, String, String, String, String, String>>() {
            public Tuple11<String, String, String, String, String, String, String, String, String, String, String> map(String s) throws Exception {
                if (!flag) {
                    ruleList = DBUtil.queryAlarmTable();
                    flag = true;
                }
                if (s != null && !"".equals(s)) {
                    String[] arrayString = StringUtils.splitByWholeSeparator(s, "(!@#$%)");
                    int length = arrayString.length;
                    Date date2 = new Date();
                    if (date2.getTime() - date1.getTime() > 1000 * 120) {
                        date1 = date2;
                        ruleList.addAll(DBUtil.queryAlarmTable());
                    }
                    if (length == 4) {
                        String message = arrayString[2];
                        String deviceIp = arrayString[0];
                        for (AlarmRule alarmRule : ruleList) {
                            String keyword = alarmRule.getKeyword();
                            if (message.indexOf(keyword) >= 0) {
                                String eventid = UUID.randomUUID().toString();
                                String eventtype = "LOG.APP";
                                String level = alarmRule.getLevel();
                                String content = alarmRule.getDescription();
                                Date date = new Date();
                                String time = format.format(date.getTime());
                                String processstatus = "0";
                                String eventstatus = "1";
                                String lasttime = time;
                                String eventtypeall = "LOG";
                                String isInsert = "0";
                                return new Tuple11<String, String, String, String, String, String, String, String, String, String, String>(eventid, eventtype
                                        , level, content, time, deviceIp, processstatus, eventstatus, lasttime, eventtypeall, isInsert);
                            }
                        }
                    }
                }
                return new Tuple11<String, String, String, String, String, String, String, String, String, String, String>("", ""
                        , "", "", "", "", "", "", "", "", "1");
            }
        });
        //WarringMessage.addSink(new MysqlSink());
        try {
            env.execute("data to mysql start");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}