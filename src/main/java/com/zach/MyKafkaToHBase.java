package com.zach;

import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class MyKafkaToHBase {
    public static void main(String[] args) {
        long events = 222228;
        //String bootstrpServerList = "yxcdh63-1.gce.cloudera.com:9092";
        //String bootstrpServerList = "10.17.101.239:9092,10.17.101.230:9092";
        String bootstrpServerList = "172.25.38.140:9092,172.25.37.12:9092";

        Boolean isSasl = false;
        String myTopic = "test.input.1";
        Integer sleep_max = 100;

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrpServerList);
        //props.put("compression.type", "gzip");
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("batch.size", 100);
        props.put("client.id", "producer-from-my-laptop");
        props.put("linger.ms", 250);
        props.put("request.timeout.ms", "300000");
        props.put("buffer.memory", 133554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
            String key = Integer.toString(msgSentCount + 2000001);
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
            String msg = transactionId + ":" + "mycol:" + System.currentTimeMillis() + "\t" + itemId + "\t" + quantity;
            ProducerRecord<String, String> myRecord = new ProducerRecord<String, String>(myTopic, key, msg);
            final String fKey = key;
            int sleepMs=rnd.nextInt(sleep_max);
            System.out.println("prep msg=" + msg); System.out.println("sleep " + sleepMs+ "ms");
            try {Thread.currentThread().sleep(sleepMs); }catch(Exception e){}
            producer.send(myRecord,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null) {
                                System.out.println("msg key=" + fKey + " lost on producer side");
                                e.printStackTrace();
                            } else {
                                System.out.println("Success offset: " + metadata.offset() + " key=" + fKey);
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
