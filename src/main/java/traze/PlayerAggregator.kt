package traze

import org.apache.flink.api.common.functions.AggregateFunction

class PlayerAggregator : AggregateFunction<Player, PlayerAggregationResult, PlayerAggregationResult> {

    override fun createAccumulator(): PlayerAggregationResult {
        return PlayerAggregationResult()
    }

    override fun add(player: Player, acc: PlayerAggregationResult): PlayerAggregationResult {
        val longPlayerName = player.name + "[" + player.id + "]"
        if (acc.scores.contains(longPlayerName)) {
            val currentScore = acc.scores.getOrElse(longPlayerName) { 0 }
            acc.scores[longPlayerName] = 1 + currentScore
        } else {
            acc.scores[longPlayerName] = player.frags
        }
        return acc
    }

    override fun merge(acc1: PlayerAggregationResult, acc2: PlayerAggregationResult): PlayerAggregationResult {
        for ((name, score) in acc1.scores){
            if (acc2.scores.contains(name)) {
                val acc2Score = acc2.scores.getOrElse(name) { 0 }
                acc2.scores[name] = score + acc2Score
            } else {
                acc2.scores[name] = score
            }
        }
        return acc2
    }

    override fun getResult(acc: PlayerAggregationResult): PlayerAggregationResult {
        return acc
    }
}