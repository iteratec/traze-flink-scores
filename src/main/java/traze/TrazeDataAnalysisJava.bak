package traze;

import mqtt.MQTTSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import traze.Player;
import traze.PlayerAggregator;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TrazeDataAnalysisJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/resources/mqtt.properties"));

        Properties mqttProperties = new Properties();
        mqttProperties.setProperty(MQTTSource.CLIENT_ID, properties.getProperty("MQTT_client_id"));
        mqttProperties.setProperty(MQTTSource.URL, properties.getProperty("MQTT_broker_address"));
        mqttProperties.setProperty(MQTTSource.TOPIC, properties.getProperty("MQTT_topic"));


        MQTTSource mqttSource = new MQTTSource(mqttProperties);
        DataStreamSource<String> mqttDataSource = env.addSource(mqttSource);
        //DataStream<String> stream = mqttDataSource.map((MapFunction<String, String>) s -> s);
        //stream.print();
        DataStream<List<Player>> playerListStream = mqttDataSource.map((String s) -> {
            ObjectMapper mapper = new ObjectMapper();
            return Arrays.asList(mapper.readValue(s, Player[].class));
        }).returns(new ListTypeInfo<>(Player.class));

        DataStream<Player> players = playerListStream.flatMap(new PlayerListSplitter());

        players
                .keyBy("name", "id")
                .timeWindow(Time.seconds(5))
                .aggregate(new PlayerAggregator())
                .print();

        env.execute("MQTT Data Analysis");
    }

    public static class PlayerListSplitter implements FlatMapFunction<List<Player>, Player> {
        @Override
        public void flatMap(List<Player> players, Collector<Player> out) throws Exception {
            for (Player player: players) {
                out.collect(player);
            }
        }
    }

}
