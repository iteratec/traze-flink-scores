FROM maven:3-jdk-8 AS builder
ADD . /scores
RUN mvn -f /scores/pom.xml clean package

FROM openjdk:8
COPY --from=builder /scores/target/traze-flink-scores-1.0.0.jar /traze-flink-scores-1.0.0.jar
ENTRYPOINT ["java","-jar","/traze-flink-scores-1.0.0.jar"]