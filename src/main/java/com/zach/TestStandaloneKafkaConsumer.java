

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;

public class TestStandaloneKafkaConsumer {

        private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        private static KafkaConsumer<String, String> consumer;
        private static class HandleRebalance implements ConsumerRebalanceListener {
            public void onPartitionsAssigned(Collection<TopicPartition>
                                                     partitions) {
            }

            public void onPartitionsRevoked(Collection<TopicPartition>
                                                    partitions) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyymmdd HH:mm:ss");
                System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " Lost partitions in rebalance. Committing current offsets:" + currentOffsets);
                consumer.commitSync(currentOffsets);
            }
        }
        // to run from command line:
//  /Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home/bin/java -Xmx2g
// -Djava.security.auth.login.config=/Users/yxzhang/Downloads/jaas.conf  -Dlog4j.configuration=./tools-log4j.properties
// -cp target/MyKafkaClients-1.0-SNAPSHOT-jar-with-dependencies.jar:/Users/yxzhang/.m2/repository/org/slf4j/slf4j-simple/1.7.7/slf4j-simple-1.7.7.jar:/Users/yxzhang/.m2/repository/org/slf4j/slf4j-api/1.7.7/slf4j-api-1.7.7.jar com.zach.MyKafkaConsumer
        public static void main(String[] args) {
            //System.out.println("Usage: java -Xmx2g -cp MyKafkaClients-1.0-SNAPSHOT-jar-with-dependencies.jar:slf4j-simple-1.7.7.jar:slf4j-api-1.7.7.jar com.zach.MyKafkaConsumer topic bootstrapServer groupId ");
            Properties props = new Properties();

            String bootstrpServerList = "c1330-node4.coelab.cloudera.com:9093";

            Boolean isSasl = true;
            Boolean isSSL = true;

            String myTopic = "testOCBC_topic";
            String prefix = "test_kafka_ ";

            String groupId="myGroup";
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "5000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "11000");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "12000");
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "15000");
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "myClient");

            if (args.length>=3) {
                groupId = trimInput(args[2]);

            }

            if (args.length >= 2) {
                bootstrpServerList = trimInput(args[1]);
            }

            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put("bootstrap.servers", bootstrpServerList);

            if (isSasl && isSSL) {
                props.put("security.protocol", "SASL_SSL");
                props.put("sasl.kerberos.service.name", "kafka");
                //to-do
                props.put("ssl.truststore.location", "my_truststore.jks");
                props.put("ssl.truststore.password", "bsi4rT0U7ep9Uv5VA4irNZNgWMPwhS626KJuZkL1klt");
                // put keystore if broker side rquire two-way auth
                props.put("ssl.keystore.location", "my_keystore.jks");
                props.put("ssl.keystore.password", "ShLvkevnq4wTOIVhJDG04EvlFqiY2OW1KLDxNEcdKTy");
            }
            else if (isSasl) {
                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("sasl.kerberos.service.name", "kafka");
            }

            consumer = new KafkaConsumer<>(props);

            if (args.length >= 1) {
                myTopic = trimInput(args[0]);
            }

            boolean noIssue = Boolean.valueOf("True");

            consumer.subscribe(Arrays.asList(myTopic), new HandleRebalance());
            int msgCount = 5;

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                // print batch processing start time:
                //System.out.println("====== Batch starts at: " + sdf.format(new Date(System.currentTimeMillis())) + " ");
                if (!records.isEmpty()) {
                    System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " fetched " + records.count() + " msgs from topic: " + myTopic);
                }
                // to simulate the group memebershipt issue:
                // sleep longer than the time out, so that we fail to send a heartbeat
                if (msgCount % 300 == 0) {
                    try {
                        System.out.println(sdf.format(new Date(System.currentTimeMillis())) +" starting to pause....");
                        Thread.currentThread().sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("%s-%s _o_: %d, _k_: %s _v_: %s \n", record.topic(),record.partition(),record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(),
                            record.partition()), new
                            OffsetAndMetadata(record.offset()+1, "no metadata"));
                    if (msgCount % 1000 == 0 && !Boolean.valueOf(props.get("enable.auto.commit").toString())) {
                        System.out.printf("Commit offset manually: %s", currentOffsets.toString());
                        consumer.commitAsync(currentOffsets, null);
                    }
                    msgCount++;
                }
                //print batch processing end time:
                // System.out.println("====== Batch ends at: " + sdf.format(new Date(System.currentTimeMillis()))) ;
            }
        }

        public static String trimInput(String input) {
            if (null != input){
                return input.trim();
            }
            return "";
        }
}
