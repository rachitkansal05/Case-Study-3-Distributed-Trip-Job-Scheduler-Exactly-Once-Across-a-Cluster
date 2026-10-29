# syntax=docker/dockerfile:1
# Notification sink image. Build context is the repository root:
#   docker build -f notification-sink/Dockerfile .
# Maven runs inside the build stage through the wrapper, so no local Maven or JDK is needed.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY scheduler-core/pom.xml scheduler-core/
COPY scheduler-app/pom.xml scheduler-app/
COPY notification-sink/pom.xml notification-sink/
COPY loadgen/pom.xml loadgen/
COPY scheduler-core/src scheduler-core/src
COPY notification-sink/src notification-sink/src
RUN --mount=type=cache,target=/root/.m2     chmod +x mvnw && ./mvnw -B -q -pl notification-sink -am package -DskipTests     && cp notification-sink/target/notification-sink-*.jar /app.jar

FROM eclipse-temurin:21-jre
RUN groupadd --system app && useradd --system --gid app --no-create-home app
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
USER app
EXPOSE 8090
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
