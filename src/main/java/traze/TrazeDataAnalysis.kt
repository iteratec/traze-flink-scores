package traze

import mqtt.MQTTSink
import mqtt.MQTTSinkConfig
import mqtt.MQTTSource
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.io.FileInputStream
import java.util.Arrays
import java.util.Properties
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory
import com.sun.org.glassfish.external.amx.AMXUtil.prop




object TrazeDataAnalysis {

    private val log = LoggerFactory.getLogger(this.javaClass)

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val parameter = ParameterTool.fromArgs(args)
        val windowSize = parameter.getLong("windowSize", 60)
        val windowSlide = parameter.getLong("windowSlide", 10)

        log.info("Starting Flink with windowSize $windowSize and windowSlide $windowSlide")

        val env = StreamExecutionEnvironment.createLocalEnvironment()

        val properties = Properties()
        properties.load(this.javaClass.getResourceAsStream("/mqtt.properties"))

        val sourceProperties = Properties()
        sourceProperties.setProperty(MQTTSource.CLIENT_ID, properties.getProperty("client_id"))
        sourceProperties.setProperty(MQTTSource.URL, properties.getProperty("broker_address"))
        sourceProperties.setProperty(MQTTSource.TOPIC, properties.getProperty("players_topic"))

        val sinkProperties = MQTTSinkConfig(
                properties.getProperty("sink_address"),
                properties.getProperty("client_id"),
                properties.getProperty("scores_topic"),
                0,
                false
        )

        val mqttSource = MQTTSource(sourceProperties)
        val mqttDataSource = env.addSource(mqttSource)

        val playerListStream = mqttDataSource.map { s: String ->
            val mapper = ObjectMapper()
            Arrays.asList(*mapper.readValue(s, Array<Player>::class.java))
        }.returns(ListTypeInfo(Player::class.java))

        val players = playerListStream.flatMap(PlayerListSplitter())

        players
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlide)))
                .aggregate(PlayerAggregator())
                .addSink(MQTTSink(sinkProperties))
                //.print()

        env.execute("TrazeDataAnalysis")
    }

    class PlayerListSplitter : FlatMapFunction<List<Player>, Player> {
        @Throws(Exception::class)
        override fun flatMap(players: List<Player>, out: Collector<Player>) {
            for (player in players) {
                out.collect(player)
            }
        }
    }

}
