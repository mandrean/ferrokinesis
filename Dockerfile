FROM scratch
ARG TARGETARCH
COPY ${TARGETARCH}/ferrokinesis /ferrokinesis
EXPOSE 4567
# Run as nobody (UID 65534) to avoid running as root.
USER 65534
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/ferrokinesis", "health-check"]
ENTRYPOINT ["/ferrokinesis"]
