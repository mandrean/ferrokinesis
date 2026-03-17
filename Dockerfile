FROM scratch
ARG TARGETARCH
COPY ${TARGETARCH}/ferrokinesis /ferrokinesis
EXPOSE 4567
ENTRYPOINT ["/ferrokinesis"]
