import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TestStandaloneKafkaProducer {
    public static void main(String[] args) {
        long events = 9;
        String bootstrpServerList = "c1330-node4.coelab.cloudera.com:9093";

        // set security per you env:
        Boolean isSasl = true;
        Boolean isSSL = true;

        String myTopic = "testOCBC_topic";
        String prefix = "test_kafka_ ";
        Integer sleep_max = 1000;


        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrpServerList);
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("batch.size", 100);
        props.put("linger.ms", 250);
        props.put("request.timeout.ms", "300000");
        props.put("buffer.memory", 133554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
        Producer<String, String> producer = new KafkaProducer<>(props);
        int msgSentCount = 0;

        int lostCount = 0;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long numItems = 1000;
        for (long nEvents = 0; nEvents < events; nEvents++) {
            String key = Integer.toString(msgSentCount + 2000001);
            long nextItemId= 0;
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
            System.out.println(" topic: " + myTopic + ", prep msg=" + msg); System.out.println("sleep " + sleepMs+ "ms");
            try {Thread.currentThread().sleep(sleepMs); }catch(Exception e){}
            producer.send(myRecord,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null) {
                                System.out.println(" topic: " + myTopic + "  msg_key=" + fKey + " lost on producer side");
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


}
