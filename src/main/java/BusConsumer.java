import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BusConsumer {
    private static final String BOOTSTRAP_SERVERS = "ec2-43-200-230-150.ap-northeast-2.compute.amazonaws.com:9092,ec2-43-202-220-253.ap-northeast-2.compute.amazonaws.com:9092,ec2-43-202-232-33.ap-northeast-2.compute.amazonaws.com:9092";
    private static final String CONSUMER_TOPIC = "jeju_produce_topic";
    private static final String PRODUCER_TOPIC = "jeju_search_topic";
    private static int recordIndex = 0; // 자동 증가할 인덱스 변수
    private static long idCounter = 1;  // 'id' 필드를 위한 카운터
    private static Map<String, double[]> previousLocations = new HashMap<>();

    public static void main(String[] args) {
        // Consumer properties 설정
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Bus");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("schema.registry.url", "http://ec2-15-164-27-43.ap-northeast-2.compute.amazonaws.com:8081,http://ec2-3-37-74-177.ap-northeast-2.compute.amazonaws.com:8081");

        // Producer properties 설정
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://ec2-15-164-27-43.ap-northeast-2.compute.amazonaws.com:8081,http://ec2-3-37-74-177.ap-northeast-2.compute.amazonaws.com:8081");

        // Avro 스키마 정의
        Schema keySchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"KeySchema\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"recordIndex\",\"type\":\"long\"}]}");
        Schema valueSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"BusData\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"ROUTE_NUM\",\"type\":\"string\"},{\"name\":\"LOCAL_Y\",\"type\":\"double\"},{\"name\":\"LOCAL_X\",\"type\":\"double\"},{\"name\":\"CURR_STATION_NM\",\"type\":\"string\"},{\"name\":\"PLATE_NO\",\"type\":\"string\"},{\"name\":\"TIMESTAMP\",\"type\":\"string\"}]}");

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(CONSUMER_TOPIC));

            while (true) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    GenericRecord busData = record.value();
                    System.out.println(busData);

                    if ("360".equals(busData.get("ROUTE_NUM").toString())) { // 필터 조건
                        String plateNo = busData.get("PLATE_NO").toString();
                        double localX = (double) busData.get("LOCAL_X");
                        double localY = (double) busData.get("LOCAL_Y");

                        // 이전 위치 확인 및 비교
                        double[] prevLocation = previousLocations.getOrDefault(plateNo, new double[]{-1, -1});
                        if (prevLocation[0] == localX && prevLocation[1] == localY) {
                            continue; // 위치가 동일하면 전송하지 않음
                        }

                        // Avro 레코드 생성
                        GenericRecord keyRecord = new GenericData.Record(keySchema);
                        keyRecord.put("id", idCounter++);
                        keyRecord.put("recordIndex", recordIndex++);

                        GenericRecord valueRecord = new GenericData.Record(valueSchema);
                        valueRecord.put("id", record.key().get("id"));
                        valueRecord.put("ROUTE_NUM", busData.get("ROUTE_NUM").toString());
                        valueRecord.put("LOCAL_Y", localY);
                        valueRecord.put("LOCAL_X", localX);
                        valueRecord.put("CURR_STATION_NM", busData.get("CURR_STATION_NM").toString());
                        valueRecord.put("PLATE_NO", plateNo);
                        valueRecord.put("TIMESTAMP", busData.get("TIMESTAMP").toString());

                        // ProducerRecord에 keyRecord와 valueRecord 전달
                        ProducerRecord<GenericRecord, GenericRecord> producerRecord =
                                new ProducerRecord<>(PRODUCER_TOPIC, keyRecord, valueRecord);

                        // Kafka로 메시지 전송
                        producer.send(producerRecord);
                        previousLocations.put(plateNo, new double[]{localX, localY});
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}