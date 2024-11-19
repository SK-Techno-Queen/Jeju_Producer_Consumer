import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class JejuBus {
    private static int recordIndex = 0; // 자동 증가할 인덱스 변수
    private static long idCounter = 1;  // 'id' 필드를 위한 카운터

    public static void main(String[] args) {
        final Properties props = new Properties() {{
            put(BOOTSTRAP_SERVERS_CONFIG, "ec2-43-200-230-150.ap-northeast-2.compute.amazonaws.com:9092,ec2-43-202-220-253.ap-northeast-2.compute.amazonaws.com:9092,ec2-43-202-232-33.ap-northeast-2.compute.amazonaws.com:9092");
            put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
            put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
            put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://ec2-15-164-27-43.ap-northeast-2.compute.amazonaws.com:8081,http://ec2-3-37-74-177.ap-northeast-2.compute.amazonaws.com:8081"); // Schema Registry URL
            put(CLIENT_ID_CONFIG, "KafkaAvroProducer");
        }};

        // Avro 키 스키마 정의 (id 추가)
        String keySchemaString = "{"
                + "\"type\": \"record\","
                + "\"name\": \"KeySchema\","
                + "\"fields\": ["
                + "{\"name\": \"id\", \"type\": \"long\"},"
                + "{\"name\": \"recordIndex\", \"type\": \"long\"}"
                + "]"
                + "}";

        // Avro 값 스키마 정의
        String schemaString = "{"
                + "\"type\": \"record\","
                + "\"name\": \"BusData\","
                + "\"fields\": ["
                + "{\"name\": \"ROUTE_NUM\", \"type\": \"string\"},"
                + "{\"name\": \"LOCAL_Y\", \"type\": \"double\"},"
                + "{\"name\": \"LOCAL_X\", \"type\": \"double\"},"
                + "{\"name\": \"CURR_STATION_NM\", \"type\": \"string\"},"
                + "{\"name\": \"PLATE_NO\", \"type\": \"string\"},"
                + "{\"name\": \"TIMESTAMP\", \"type\": \"string\"}"
                + "]"
                + "}";

        // 스키마 객체 생성
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        Schema keySchema = new Schema.Parser().parse(keySchemaString);

        final String topic = "bus_produce_topic";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (final Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (true) {
                try {
                    StringBuilder urlBuilder = new StringBuilder("http://bus.jeju.go.kr/api/searchBusAllocList.do");

                    URL url = new URL(urlBuilder.toString());
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("GET");
                    conn.setRequestProperty("Content-type", "application/json");

                    int responseCode = conn.getResponseCode();
                    System.out.println("Response code: " + responseCode);

                    BufferedReader rd;
                    if (responseCode >= 200 && responseCode <= 300) {
                        rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    } else {
                        rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                    }

                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = rd.readLine()) != null) {
                        response.append(line);
                    }
                    rd.close();
                    conn.disconnect();

                    JSONArray busDataArray = new JSONArray(response.toString());

                    for (int i = 0; i < busDataArray.length(); i++) {
                        JSONObject busData = busDataArray.getJSONObject(i);

                        if ("360".equals(busData.getString("ROUTE_NUM"))) {
                            String currentTime = LocalDateTime.now().format(formatter);
                            System.out.println("현재 시간: " + currentTime);
                            System.out.println("버스 번호: " + busData.getString("ROUTE_NUM"));
                            System.out.println("위치 (Y): " + busData.getDouble("LOCAL_Y"));
                            System.out.println("위치 (X): " + busData.getDouble("LOCAL_X"));
                            System.out.println("정류장 이름: " + busData.getString("CURR_STATION_NM"));
                            System.out.println("=================================");

                            // Avro 레코드를 위한 객체 생성 (값)
                            GenericRecord record = new GenericData.Record(avroSchema);
                            record.put("ROUTE_NUM", busData.getString("ROUTE_NUM"));
                            record.put("LOCAL_Y", busData.getDouble("LOCAL_Y"));
                            record.put("LOCAL_X", busData.getDouble("LOCAL_X"));
                            record.put("CURR_STATION_NM", busData.getString("CURR_STATION_NM"));
                            record.put("PLATE_NO", busData.getString("PLATE_NO"));
                            record.put("TIMESTAMP", currentTime);
                            System.out.println(record);
                            System.out.println("===============================");

                            // 자동 증가하는 인덱스를 키로 사용 (키는 Avro 형식으로 설정)
                            GenericRecord keyRecord = new GenericData.Record(keySchema);
                            keyRecord.put("id", idCounter++);  // 'id' 필드 추가
                            keyRecord.put("recordIndex", recordIndex++);  // 자동 증가 인덱스

                            // Avro 키와 값으로 ProducerRecord 생성
                            ProducerRecord<GenericRecord, GenericRecord> producerRecord =
                                    new ProducerRecord<>(topic, keyRecord, record);
                            producer.send(producerRecord);
                        }
                    }

                    Thread.sleep(1000);

                } catch (IOException e) {
                    System.out.println("API 요청 중 오류 발생: " + e.getMessage());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}