# Traze Flink Scores

Apache Flink stack that consumes traze MQTT topics, computes scores on a per window basis, and publishes the results at `traze/*/scores`. For now, just one game instance is active, i.e. `traze/1/scores`