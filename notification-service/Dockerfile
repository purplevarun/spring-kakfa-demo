FROM maven:3.9-eclipse-temurin-26 AS build

WORKDIR /build
COPY . .
RUN mvn clean install

FROM eclipse-temurin:26-jre

WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
EXPOSE 4002
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
