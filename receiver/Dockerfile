# Imagem de execução do receiver. O jar é construído fora, para a imagem ser pequena e o build ser rápido:
#   ./mvnw -pl receiver -am package -DskipTests
#   docker compose --profile app up -d --build receiver
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/receiver-*-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
