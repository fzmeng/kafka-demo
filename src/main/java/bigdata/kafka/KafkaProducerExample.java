package bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author mengfanzhu
 * @Package bigdata.kafka
 * @Description: kafka生产者
 * @date 17/3/8 17:20
 */
public class KafkaProducerExample {

    public void produceMessage()
    {
        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        String topic="slavetest",key,value;
        for (int i = 0; i < 1000; i++) {
            key = "key"+i;
            value="value"+i;
            System.out.println("TOPIC: slavetest;发送KEY："+key+";Value:"+value);
            producer.send(new ProducerRecord<String, String>(topic, key,value));
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

    // config
    public Properties getConfig()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.211.55.3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void main(String[] args)
    {
        KafkaProducerExample example = new KafkaProducerExample();
        example.produceMessage();
    }
}
