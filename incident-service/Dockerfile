# Build from the repo root so the parent pom is in context:
#   docker build -f incident-service/Dockerfile .
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY incident-service/pom.xml incident-service/pom.xml
COPY dispatch-service/pom.xml dispatch-service/pom.xml
COPY incident-service/src incident-service/src
RUN mvn -q -B -pl incident-service -am -DskipTests package

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 1001 appuser
WORKDIR /app
COPY --from=build /app/incident-service/target/incident-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
