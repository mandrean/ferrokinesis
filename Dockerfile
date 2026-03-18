FROM scratch
ARG TARGETARCH
COPY ${TARGETARCH}/ferrokinesis /ferrokinesis
EXPOSE 4567
# Run as nobody (UID 65534) to avoid running as root.
# Note: HEALTHCHECK will be enabled once the built-in health endpoint (GET /_health)
# is implemented in #68. Until then, add a healthcheck in your docker-compose.yml:
#   healthcheck:
#     test: ["CMD", "curl", "-f", "http://localhost:4567/_health"]
#     interval: 10s
#     timeout: 3s
#     retries: 3
USER 65534
ENTRYPOINT ["/ferrokinesis"]
