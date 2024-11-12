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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class BusSearch {
    private static int recordIndex = 0; // 자동 증가할 인덱스 변수
    private static long idCounter = 1;  // 'id' 필드를 위한 카운터
    private static Map<String, double[]> previousLocations = new HashMap<>();

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
                + "{\"name\": \"id\", \"type\": \"long\"},"               // 'id' 필드 추가
                + "{\"name\": \"ROUTE_NUM\", \"type\": \"string\"},"     // 버스 번호
                + "{\"name\": \"LOCAL_Y\", \"type\": \"double\"},"       // 위도
                + "{\"name\": \"LOCAL_X\", \"type\": \"double\"},"       // 경도
                + "{\"name\": \"CURR_STATION_NM\", \"type\": \"string\"}," // 정류장 이름
                + "{\"name\": \"PLATE_NO\", \"type\": \"string\"},"      // 버스 번호판
                + "{\"name\": \"TIMESTAMP\", \"type\": \"string\"}"      // 시간
                + "]"
                + "}";

        // 스키마 객체 생성
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        Schema keySchema = new Schema.Parser().parse(keySchemaString);

        final String topic = "topic_82";

        try (final Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (true) {
                try {
                    // API 요청
                    StringBuilder urlBuilder = new StringBuilder("http://localhost:8080/bus");
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

                    // JSON 응답을 파싱
                    JSONArray busDataArray = new JSONArray(response.toString());

                    for (int i = 0; i < busDataArray.length(); i++) {
                        JSONObject busData = busDataArray.getJSONObject(i);

                        if ("360".equals(busData.getString("routeNum"))) {  // 필터링 조건
                            String plateNo = busData.getString("plateNo");
                            double localX = busData.getDouble("localX");
                            double localY = busData.getDouble("localY");

                            if (previousLocations.containsKey(plateNo)) {
                                double[] prevLocation = previousLocations.get(plateNo);

                                // 위치가 변경되었는지 확인
                                if (prevLocation[0] == localX || prevLocation[1] == localY) {
                                    // 위치가 동일하면 전송하지 않음
                                    continue;
                                }
                            }

                            // Avro 레코드 값 생성
                            GenericRecord record = new GenericData.Record(avroSchema);
                            record.put("id", busData.getLong("id"));
                            record.put("ROUTE_NUM", busData.getString("routeNum"));
                            record.put("LOCAL_Y", busData.getDouble("localY"));
                            record.put("LOCAL_X", busData.getDouble("localX"));
                            record.put("CURR_STATION_NM", busData.getString("currStationNm"));
                            record.put("PLATE_NO", busData.getString("plateNo"));
                            record.put("TIMESTAMP", busData.getString("timestamp"));

                            // 키 레코드 생성
                            GenericRecord keyRecord = new GenericData.Record(keySchema);
                            keyRecord.put("id", idCounter++);
                            keyRecord.put("recordIndex", recordIndex++);

                            // Kafka로 Avro 레코드 전송
                            ProducerRecord<GenericRecord, GenericRecord> producerRecord =
                                    new ProducerRecord<>(topic, keyRecord, record);
                            producer.send(producerRecord);

                            previousLocations.put(plateNo, new double[]{localX, localY});
                        }
                    }

                    Thread.sleep(2500);

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