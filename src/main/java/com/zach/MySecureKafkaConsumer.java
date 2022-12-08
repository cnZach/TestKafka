package com.zach;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;

public class MySecureKafkaConsumer {
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private static KafkaConsumer<String, String> consumer;
    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition>
                                                 partitions) {
        }

        public void onPartitionsRevoked(Collection<TopicPartition>
                                                partitions) {
            SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
            System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " Lost partitions in rebalance. Committing current offsets:" + currentOffsets);
            consumer.commitSync(currentOffsets);
        }
    }

    public static void main(String[] args) {
        System.out.println("Usage: java -Xmx2g -cp MyKafkaClients-1.0-SNAPSHOT-jar-with-dependencies.jar:slf4j-simple-1.7.7.jar:slf4j-api-1.7.7.jar com.zach.MySecureKafkaConsumer topic bootstrapServer groupId SASL_PLAINTEXT");

        Properties props = new Properties();
        //String bootstrpServerList = "nightly57-unsecure-1.gce.cloudera.com:9092,nightly57-unsecure-2.gce.cloudera.com:9092,nightly57-unsecure-3.gce.cloudera.com:9092";
        //String bootstrpServerList = "yxcdp714-2.gce.cloudera.com:9092";
        String bootstrpServerList = "10.17.101.132:9092";
        String securityProtocol = "PLAINTEXT";
        String groupId="myGroup";
        props.put("bootstrap.servers", bootstrpServerList);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "5000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "11000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "12000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");

        if(args.length >= 4) {
            groupId = trimInput(args[3]);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        if(args.length >= 3) {
            securityProtocol = trimInput(args[2]);
            props.put("security.protocol", securityProtocol);
            if (securityProtocol.contains("SASL")) {
                props.put("sasl.enabled.mechanisms","GSSAPI");
            }
        }
        props.put("security.protocol", securityProtocol);

        props.put("sasl.kerberos.service.name","kafka");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "testClient");

        String myTopic = "test_topic";

        if (args.length >= 2) {
            bootstrpServerList = trimInput(args[1]);
        }
        props.put("bootstrap.servers", bootstrpServerList);

        consumer = new KafkaConsumer<>(props);


        if (args.length >= 1) {
            myTopic = trimInput(args[0]);
        }

        consumer.subscribe(Arrays.asList(myTopic), new HandleRebalance());
        int msgCount = 5;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(500);
            // print batch processing start time:
            // System.out.println("====== Batch starts at: " + sdf.format(new Date(System.currentTimeMillis())) + " ");
            // if (!records.isEmpty()) {
            //    System.out.println("== "+ sdf.format(new Date(System.currentTimeMillis())) + " fetched " + records.count() + " msgs from topic: " + myTopic);
            // }
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
                System.out.printf(" %s-%s offset: %d, _k_: %s _v_: %s \n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                currentOffsets.put(new TopicPartition(record.topic(),
                        record.partition()), new
                        OffsetAndMetadata(record.offset()+1, "no metadata"));
                if (msgCount % 1000 == 0 && !Boolean.valueOf(props.get("enable.auto.commit").toString())) {
                    System.out.printf("going to commit offset manually: %s", currentOffsets.toString());
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