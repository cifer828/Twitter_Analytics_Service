FROM openjdk:8-jdk-alpine
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
ENV MYSQL_DB=
ENV MYSQL_USER=
ENV MYSQL_PWD=
ENTRYPOINT ["java","-jar","/app.jar"]