package traze

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper

data class PlayerAggregationResult(
        var scores: HashMap<String, Int> = hashMapOf()
) {
    override fun toString(): String {
        val mapper = ObjectMapper()
        return mapper.writeValueAsString(scores)
    }
}