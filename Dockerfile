FROM amazoncorretto:17 AS builder

# 베이스 이미지에 xargs(findutils)가 없어 gradlew 실행이 실패하므로 설치
RUN yum install -y findutils && yum clean all

WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts settings.gradle.kts ./

RUN chmod +x ./gradlew && ./gradlew dependencies --no-daemon || true

COPY src src
RUN ./gradlew clean bootJar -x test --no-daemon

# ===== 실행 스테이지 =====
FROM amazoncorretto:17-alpine

RUN apk add --no-cache curl

WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8081

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]