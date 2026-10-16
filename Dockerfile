FROM eclipse-temurin:23-jdk-alpine AS build
WORKDIR /app

RUN apk add --no-cache maven

COPY pom.xml .
COPY src src

RUN mvn -B package -DskipTests

FROM eclipse-temurin:23-jre-alpine
WORKDIR /app

# Keep the runtime identity stable so Kubernetes can grant write access to /data
# without running the server as root.
RUN addgroup -S -g 10001 redisforge \
    && adduser -S -D -H -u 10001 -G redisforge redisforge \
    && mkdir -p /data \
    && chown -R redisforge:redisforge /app /data

COPY --from=build --chown=redisforge:redisforge /app/target/redisforge.jar ./redisforge.jar

USER 10001:10001

EXPOSE 6379

ENTRYPOINT ["java", "-jar", "redisforge.jar"]
