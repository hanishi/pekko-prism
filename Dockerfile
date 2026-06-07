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
# Distroless: a real JVM (so Pekko works unchanged) on a minimal base with no shell or
# package manager, which shrinks the image and cuts the CVE/attack surface. Debug with
# an ephemeral container (kubectl debug / docker run --image=busybox), not `exec sh`.
FROM gcr.io/distroless/java21-debian12

WORKDIR /app
COPY target/scala-3.3.4/prism-proxy.jar /app/prism-proxy.jar

# Non-root (numeric uid; matches the manifests' runAsUser: 1000).
USER 1000

EXPOSE 8080

# Config is injected, not baked: with no argument the proxy runs on the built-in
# application.conf defaults; pass a mounted file path to override (Kubernetes mounts
# the ConfigMap and passes /config/proxy.conf). MaxRAMPercentage so the JVM honours
# the container memory limit.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/prism-proxy.jar"]
