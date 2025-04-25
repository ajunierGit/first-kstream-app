FROM maven:3.9.5-eclipse-temurin-17 as builder
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17
WORKDIR /app
COPY --from=builder /app/target/first-kstreams-app-1.0-SNAPSHOT.jar app.jar
CMD ["java", "-jar", "app.jar"]