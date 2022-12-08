package com.zach;
import java.util.*;
import org.apache.kafka.clients.producer.*;

import java.util.concurrent.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.math3.distribution.ParetoDistribution;

public class MyKafkaProducer {
    public static void main(String[] args) {
        long events = 10;
        String bootstrpServerList = "172.25.42.142:9092";
        //String bootstrpServerList = "yxcdp714-2.gce.cloudera.com:9094";
        //String bootstrpServerList = "yxzcdh-1.gce.cloudera.com:9094" ;
        //,yxzcdh-1.gce.cloudera.com:9094";
        //String bootstrpServerList = "c1330-node2.coelab.cloudera.com:9094";
        Boolean isSasl = false;
        String myTopic = "testtopic";
        //String myTopic = "test_topic_cloudera_hdfs_20201209";
        String prefix = "_cdp_ ";
        Integer sleep_max = 1000;
        if (args.length >= 1) {
            myTopic = args[0];
        }
        if (args.length >= 2) {
            bootstrpServerList = args[1];
        }
        /*
        if (args.length > 2) {
            myTopic = args[0];
            events = Long.parseLong(args[1]);
            bootstrpServerList = args[2];
            isSasl = Boolean.parseBoolean(args[3]);
            prefix = args[4];
        } else {
            System.out.pringln("Wrong arg[] size:");
            System.out.println(" arg0: topic       :string, ");
            System.out.println(" arg1: num of msgs :int, ");
            System.out.println(" arg2: broker-list :string, ");
            System.out.println(" arg3: sasl or not :boolean, ");
            System.out.println(" arg4: msg prefix  :string ");
        }
        */

        Properties props = new Properties();
        //String bootstrpServerList = "nightly57-unsecure-1.gce.cloudera.com:9092,nightly57-unsecure-2.gce.cloudera.com:9092,nightly57-unsecure-3.gce.cloudera.com:9092";
        //bootstrpServerList = "10.17.81.203:9092,10.17.80.111:9092,10.17.81.210:9092";
        props.put("bootstrap.servers", bootstrpServerList);
        //props.put("compression.type", "gzip");
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("batch.size", 100);
        props.put("client.id", "test-from-IDE");
        props.put("linger.ms", 250);
        props.put("request.timeout.ms", "300000");
        props.put("buffer.memory", 133554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "15000");
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.zach.RoundRobinPartitioner");
        if (isSasl) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.kerberos.service.name", "kafka");
        }

        Producer<String, String> producer = new KafkaProducer<>(props);
        int msgSentCount = 0;

        int lostCount = 0;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long numItems = 1000;
        ParetoDistribution paretoDistribution = new ParetoDistribution(numItems, 15);
        for (long nEvents = 0; nEvents < events; nEvents++) {
            String key = genereateRandomString(msgSentCount % 10);
            key = "same_key";
            long nextItemId;
            do {
                nextItemId = sample(paretoDistribution);
            } while (nextItemId > numItems);
            String itemId = "item_" + nextItemId;

            int quantity = (int) (Math.round(rnd.nextGaussian() / 2 * 10) * 10) + 5;
            if (quantity == 0) {
                continue;
            }
            long transactionId = rnd.nextLong(Long.MAX_VALUE);
            String msg = transactionId + "\t" + System.currentTimeMillis() + "\t" + itemId + "\t" + quantity;
            ProducerRecord<String, String> myRecord = new ProducerRecord<String, String>(myTopic, key, msg);
            final String fKey = key;
            int sleepMs=rnd.nextInt(sleep_max);
            msg="{\"name\":\"" + fKey +"\",\"temp\":" + transactionId + "}";
            System.out.println("prep msg=" + msg); System.out.println("sleep " + sleepMs+ "ms");
            try {Thread.currentThread().sleep(sleepMs); }catch(Exception e){}
            producer.send(myRecord,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null) {
                                System.out.println("msg key=" + fKey + " lost on producer side");
                                e.printStackTrace();
                            } else {
                                System.out.println("Success offset: " + metadata.offset() + " key=" + fKey + " topic=" + metadata.topic() + ":"
                                        + metadata.partition() );
                            }
                        }
                    });
            msgSentCount ++;
            if (msgSentCount % 1000 == 0) {
                System.out.println("sending key=" + key + " msg to topic " + myTopic);
            }
        }
        try {
            System.out.println("sleeping...");
            Thread.currentThread().sleep(10);
        } catch (Exception e) {

        }
        producer.close();
        System.out.println("sent " + msgSentCount + " msgs to topic " + myTopic + " producer props: " + props.toString());
    }

    public static String genereateRandomString(Integer len) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = len;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();
        return generatedString;
    }

    private static long sample(ParetoDistribution paretoDistribution) {
        return (Math.round(paretoDistribution.sample() - paretoDistribution.getScale()) + 1);
    }

}
