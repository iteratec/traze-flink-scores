package mqtt

data class MQTTSinkConfig(
var uriArg: String,
var clientIdPrefixArg: String,
var topicArg: String,
var qosArg: Int,
var retainedArg: Boolean
)