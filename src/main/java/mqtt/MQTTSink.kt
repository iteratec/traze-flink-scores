package mqtt

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.slf4j.LoggerFactory
import traze.PlayerAggregationResult
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback



class MQTTSink(config: MQTTSinkConfig) : RichSinkFunction<PlayerAggregationResult>(), MqttCallback {

    private val log = LoggerFactory.getLogger(this.javaClass)
    private var client: MqttClient? = null
    private val clientIdPrefix: String = config.clientIdPrefixArg
    private val brokerAddress: String = config.uriArg
    private val topic: String = config.topicArg
    private val qos: Int = config.qosArg
    private val retained: Boolean = config.retainedArg
    private var callback: MqttCallback? = null

    private fun connect() {
        val connectOptions = MqttConnectOptions()
        connectOptions.isCleanSession = true

        val genClientId = "%s_%04d%05d".format(
                clientIdPrefix,
                Thread.currentThread().id % 10000,
                System.currentTimeMillis() % 100000
        )
        client = MqttClient(brokerAddress, genClientId, MemoryPersistence())

        client?.connect(connectOptions)
        client?.setCallback(callback)

        log.info("connected to mqtt broker $brokerAddress from sink")
    }

    override fun open(parameters: Configuration) {
        super.open(parameters)
        callback = object : MqttCallback {
            override fun connectionLost(t: Throwable) {
                this@MQTTSink.connect()
            }

            @Throws(Exception::class)
            override fun messageArrived(topic: String, message: MqttMessage) {}
            override fun deliveryComplete(token: IMqttDeliveryToken) {}
        }

        connect()
    }

    override fun invoke(result: PlayerAggregationResult, context: SinkFunction.Context<*>) {
        try {
            if (!client!!.isConnected) {
                throw Exception("MqttClient is not connected")
            }
            val bytes = result.toString().toByteArray(Charsets.UTF_8)
            log.info("Publishing ${bytes.toString(Charsets.UTF_8)} to $topic")
            client?.publish(topic, bytes, qos, retained)
        } catch (e: Exception) {
            throw RuntimeException("Failed to publish", e)
        }
    }

    override fun close() {
        super.close()
        client?.close()
    }

    override fun messageArrived(topic: String, message: MqttMessage): Unit {}
    override fun connectionLost(cause: Throwable) {throw cause}
    override fun deliveryComplete(t: IMqttDeliveryToken) {}
}