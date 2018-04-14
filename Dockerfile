FROM openjdk:8-jdk-alpine
MAINTAINER feryandi

# Install maven
# RUN apk update && apk add maven

WORKDIR /
COPY . $WORKDIR

EXPOSE 8080

# RUN mvn clean install

WORKDIR /boilerpipe-service
# RUN mvn clean compile assembly:single

CMD ["java", "-jar", "target/boilerpipe-service-0.1-jar-with-dependencies.jar"]  
