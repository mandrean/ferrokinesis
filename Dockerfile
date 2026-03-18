FROM scratch
ARG TARGETARCH
COPY ${TARGETARCH}/ferrokinesis /ferrokinesis
EXPOSE 4567
# Run as nobody (UID 65534) to avoid running as root.
# Note: HEALTHCHECK requires a health endpoint (#68). Until then, use http://localhost:4567.
USER 65534
ENTRYPOINT ["/ferrokinesis"]
