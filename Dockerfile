FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package && cp target/trip-job-scheduler-*.jar /app.jar

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 scheduler
COPY --from=build /app.jar /app/app.jar
USER scheduler
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
