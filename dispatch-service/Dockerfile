# Build from the repo root so the parent pom is in context:
#   docker build -f dispatch-service/Dockerfile .
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY incident-service/pom.xml incident-service/pom.xml
COPY dispatch-service/pom.xml dispatch-service/pom.xml
COPY dispatch-service/src dispatch-service/src
RUN mvn -q -B -pl dispatch-service -am -DskipTests package

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 1001 appuser
WORKDIR /app
COPY --from=build /app/dispatch-service/target/dispatch-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
