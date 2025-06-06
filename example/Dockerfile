# Use the official Flink 2.0 base image
FROM flink:2.0


ENV FLINK_VERSION=2.0.0

# Create plugin directory and download the S3 plugin
RUN mkdir -p /opt/flink/plugins/s3 && \
    curl -L -o /opt/flink/plugins/s3/flink-s3-fs-hadoop-2.0.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/2.0.0/flink-s3-fs-hadoop-2.0.0.jar
