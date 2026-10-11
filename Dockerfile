FROM eclipse-temurin:23-jdk-alpine AS build
WORKDIR /app

RUN apk add --no-cache maven

COPY pom.xml .
COPY src src

RUN mvn -B package -DskipTests

FROM eclipse-temurin:23-jre-alpine
WORKDIR /app

# the server runs as its own user rather than root, so a process that is
# compromised does not get the container's root. The append-only file is written
# under /data when persistence is wanted; /app stays writable too, for a server
# run with the default --appendfilename.
RUN addgroup -S redisforge && adduser -S -G redisforge redisforge \
    && mkdir -p /data \
    && chown -R redisforge:redisforge /app /data

COPY --from=build /app/target/redisforge.jar .

USER redisforge

EXPOSE 6379

ENTRYPOINT ["java", "-jar", "redisforge.jar"]