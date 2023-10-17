FROM cricketeerone/apache-kafka-connect:latest

ADD ./target/*.jar /app/libs/
ADD ./libs/*.jar /app/libs/