# One image for both tiers. The role is chosen at run time:
#   API tier:    JOBS_WORKER_ENABLED=false
#   Worker tier: JOBS_API_ENABLED=false

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 jobs
WORKDIR /app
COPY --from=build /build/target/async-job-service.jar app.jar
USER jobs
EXPOSE 8080
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
