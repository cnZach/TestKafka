/**
 * Created by yxzhang on 5/25/16.
 */

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;

public class MyKafkaConsumerLongBatchProcessing {
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private static KafkaConsumer<String, String> consumer;
    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition>
                                                 partitions) {
        }

        public void onPartitionsRevoked(Collection<TopicPartition>
                                                partitions) {
            SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
            System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " Initial join or Just lost partitions in rebalance. Committing current offsets:" + currentOffsets);
            //consumer.commitSync(currentOffsets);
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        //String bootstrpServerList = "nightly57-unsecure-1.gce.cloudera.com:9092,nightly57-unsecure-2.gce.cloudera.com:9092,nightly57-unsecure-3.gce.cloudera.com:9092";
        String bootstrpServerList = "yxcdh513-1.gce.cloudera.com:9092";
        props.put("bootstrap.servers", bootstrpServerList);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "5000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "11000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-from_mac" + String.valueOf(System.currentTimeMillis()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testR_Group_A");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "DFW_MM-0");
        consumer = new KafkaConsumer<>(props);
        String myTopic = "testR";
        consumer.subscribe(Arrays.asList(myTopic), new HandleRebalance());
        int msgCount = 5;

        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(900);
            // print batch processing start time:
            System.out.println("====== Batch starts at: " + sdf.format(new Date(System.currentTimeMillis())) + " ");
            // to simulate the group memebershipt issue:
            // sleep longer than the time out, so that we fail to send a heartbeat
            if (msgCount > 3) {
                try {
                    System.out.println("  start to pause...at : " + sdf.format(new Date(System.currentTimeMillis())));
                    Thread.currentThread().sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (ConsumerRecord<String, String> record : records) {
                currentOffsets.put(new TopicPartition(record.topic(),
                        record.partition()), new
                        OffsetAndMetadata(record.offset()+1, "no metadata"));
                // if not auto commit, then commit every 1000 msgs manually
                if (msgCount % 1000 == 0 && !Boolean.valueOf(props.get("enable.auto.commit").toString())) {
                    System.out.printf("going to commit offset manually: %s", currentOffsets.toString());
                    consumer.commitAsync(currentOffsets, null);
                }
                msgCount++;
            }
            //print batch processing end time:
            System.out.println("====== Batch   ends at: " + sdf.format(new Date(System.currentTimeMillis()))) ;
        }
    }
}