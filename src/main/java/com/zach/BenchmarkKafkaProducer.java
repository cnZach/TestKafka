package com.zach;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class BenchmarkKafkaProducer {
    public static String TOPIC = null;

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        int noOfMsgs = 900000;
        TOPIC = "test1";
        if (args.length > 0) {
            TOPIC = args[0];
            noOfMsgs = Integer.parseInt(args[1]);
        }
        Properties prop = new Properties();
        //prop.put("security.protocol", "SASL_PLAINTEXT");
        //prop.put("sasl.kerberos.service.name", "kafka");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.112.88:9092");
        prop.put(ProducerConfig.RETRIES_CONFIG, 3);

//        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 104857600);
//        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
//        prop.put(ProducerConfig.LINGER_MS_CONFIG, 30);
//        prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
//        prop.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
//        prop.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 1048576);
//        prop.put(ProducerConfig.SEND_BUFFER_CONFIG, 1048576);
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        //System.setProperty("java.security.auth.login.config", "/home/kafka_client_jaas.conf");
        //System.setProperty("java.security.krb5.conf", "/data/kdeenath/KafkaShared/krb5.conf");
        Producer<String, String> oProducer = new KafkaProducer<String, String>(prop);
        long time1 = 0L;
        long time2 = 0L;

        System.out.println("START");
        int count = 600000;

        String msg = "{\"e\":{\"dt\":\"EMAIL\",\"d\":\"IyYIaz5M4uA8ccqA8V73mG_2arqIJqxcyUhuEy40Cc0AAwIyYIaz5M4uA8ccqA8V73mG_2arqIJqxcyUhuEy40Cc0AAwIyYIaz5M4uA8ccqA8V73mG_2arqIJqxcyUhuEy40Cc0AAwIyYIaz5M4uA8ccqA8V73mG_2arqIJqxcyUhuEy40Cc0AAwIyYIaz5M4uA8ccqA8V73mG_2arqIJqxcyUhuEy40Cc0AAwIyYIaz5M4uA8ccqA8V73mG_2arqIJqxcyUhuEy40Cc0AAw\",\"ap\":\"kdeenath\",\"ch\":\"10.211.63.42\",\"o\":\"DECRYPT_NON_PAN\",\"ot\":\"1467929832016\",\"sh\":\"10.211.63.42\",\"sv\":\"1.2\"}}";

        time1 = System.currentTimeMillis();
        System.out.println("producer =" + oProducer.toString());

        while (noOfMsgs > count) {
            //oProducer.send(new ProducerRecord<String, String>(TOPIC, msg));
            System.out.print(count + ": ");
            ProducerRecord<String, String> myRecord = new ProducerRecord<String, String>(TOPIC, String.valueOf(count), msg);
            final String fKey = String.valueOf(count);
            oProducer.send(myRecord,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null) {
                                System.out.println("msg key=" + fKey + " lost on producer side");
                                e.printStackTrace();
                            }
                            System.out.println("Success offset: " + metadata.offset() + " key=" + fKey);
                        }
                    });
            //System.out.print("done" + ",");
            count++;
        }

        time2 = System.currentTimeMillis();
        System.out.println("Start Time: " + time1 + ", End Time: " + time2);
        System.out.println("Throughput (ops/sec) for " + count + " records (" + (time2 - time1) + ") = " + count * 1.0E3 / (time2 - time1));
        oProducer.close();
        System.out.println("END");
    }
}
