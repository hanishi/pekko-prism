# Runtime image for the prism proxy.
#
# Build the fat jar first, then the image:
#   sbt assembly
#   docker build -t prism-proxy:latest .
#   docker run --rm -p 8080:8080 -v "$PWD/proxy.conf:/config/proxy.conf" prism-proxy:latest /config/proxy.conf
#
# This single-stage variant keeps the runtime image small and matches the common CI
# pattern (build the artifact, then containerize). For a self-contained build that
# compiles inside Docker, use a multi-stage build with an sbt base image instead.
FROM eclipse-temurin:21-jre

WORKDIR /app
COPY target/scala-3.3.4/prism-proxy.jar /app/prism-proxy.jar
COPY proxy.conf /app/proxy.conf

# Run as a non-root user. eclipse-temurin already provides uid 1000; reuse it
# (the Kubernetes manifests also pin runAsUser: 1000 / runAsNonRoot).
USER 1000

EXPOSE 8080

# MaxRAMPercentage so the JVM honours the container memory limit. The config path is
# the final argument; in Kubernetes it is overridden to the mounted ConfigMap path.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/prism-proxy.jar"]
CMD ["/app/proxy.conf"]
