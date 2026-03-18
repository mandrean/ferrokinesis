FROM scratch
ARG TARGETARCH
COPY ${TARGETARCH}/ferrokinesis /ferrokinesis
EXPOSE 4567
# Run as nobody (UID 65534) to avoid running as root.
# Note: HEALTHCHECK is not supported in FROM scratch images without additional
# binaries (no shell, curl, wget, or nc available). Users can add a healthcheck
# in their docker-compose.yml, e.g.:
#   healthcheck:
#     test: ["CMD", "curl", "-f", "http://localhost:4567/"]
#     interval: 10s
#     timeout: 3s
#     retries: 3
USER 65534
ENTRYPOINT ["/ferrokinesis"]
