package kafka.producer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;

public class KafkaStreamProducer {

    private static final String API_URL = "https://api.coinranking.com/v2/coins?timePeriod=1h";
    private static final String TOPIC_NAME = "crypto";
    private static final Integer TIME_STAMP = 15000;


    static String urlparam;

   // private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Producer<String, String> producer = createKafkaProducer();


    private static Producer<String, String> createKafkaProducer() {
        // Configure Kafka producer
        Properties properties = new Properties();
        // Assigner l'identifiant du serveur kafka
        properties.put("bootstrap.servers", "kafka:9092");//"localhost:9092");
        // Definir un acquittement pour les requetes du producteur
        properties.put("acks", "all");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(properties);
       return producer;
    }

    
    private static JSONObject fetchCoinsFromAPI() throws IOException {
        URL url = new URL(API_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json");
    
        StringBuilder response = new StringBuilder();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
        }
    
        return new JSONObject(response.toString()).getJSONObject("data");
    }


    
private static Map<String, Object> parseCoinData(JSONObject coinData) {
    try {
        Map<String, Object> parsedData = new HashMap<>();
        parsedData.put("name_coin", coinData.getString("name"));
        parsedData.put("symbol_coin", coinData.getString("symbol"));
        parsedData.put("id", coinData.getString("uuid"));
        parsedData.put("volume", coinData.getBigDecimal("24hVolume"));
        parsedData.put("market_cap", coinData.getBigDecimal("marketCap"));
        parsedData.put("price", coinData.getBigDecimal("price"));
        parsedData.put("percent_change_24hr", coinData.getDouble("change"));
        parsedData.put("time_stamp", System.currentTimeMillis() / 1000);//long unixTime = System.currentTimeMillis() / 1000;
        
        JSONArray sparkline = coinData.getJSONArray("sparkline");
        if (!sparkline.isEmpty() && sparkline.length() > 1) {
            BigDecimal price1 = sparkline.getBigDecimal(0);
            //double price2 = sparkline.getDouble(1);
            
            parsedData.put("price_old", price1);
            //parsedData.put("price_-2", price2);
        } else {
            parsedData.put("price_old", coinData.getBigDecimal("price"));
            //parsedData.put("price_-2", coinData.getDouble("price"));
        }

        return parsedData;
    } catch (Exception e) {
        e.printStackTrace();
        return null;
    }
}

    public static void main(String[] args) throws InterruptedException {
             if (args.length > 0) {
                 
                if (args[0] != null && args[0].equals(0)) 
                urlparam = API_URL +"?timePeriod=1h";
                else
                urlparam = API_URL +"?timePeriod=24h";}


            try {

                while (true) {
                    try{
                    JSONObject data = fetchCoinsFromAPI();
                    if (data != null) {
                        JSONArray coins = data.getJSONArray("coins");
                        if (coins != null) {
                        coins.forEach(coin -> {
                            JSONObject coinData = (JSONObject) coin;
                            Map<String, Object> message = parseCoinData(coinData);
                            //add total 24hvolumne to the message
                            message.put("total_24_volume", data.getJSONObject("stats").getBigDecimal("total24hVolume"));
                            message.put("total_market_cap", data.getJSONObject("stats").getBigDecimal("totalMarketCap"));
                            if (message != null) {
                                System.out.println("Sending message: " + new JSONObject(message).toString());
                                producer.send(new ProducerRecord<>(TOPIC_NAME, coinData.getString("uuid"), new JSONObject(message).toString()));
                            }
                        });
                        producer.flush();
                    }}
                    Thread.sleep(TIME_STAMP); // Sleep for 10 seconds
                }  catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(TIME_STAMP);
                    continue; 
                } 
                }

            } catch (Exception e) {
                e.printStackTrace(); 
            } finally {
                producer.close();
            }
}
}

/*
 *      
        name_coin      string
        symbol_coin    string
        id             string
        volume         bigdecimal
        market_cap     bigdecimal
        price          bigdecimal
        price_old       bigdecimal
        percent_change_24hr double
        time_stamp      timestamp
        total_24_volume bigdecimal
        total_market_cap bigdecimal
 * 
 * 
 */