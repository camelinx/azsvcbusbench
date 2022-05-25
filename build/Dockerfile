FROM alpine:3.15

LABEL maintainer="Sudarshan Raghavan <camelinx@gmail.com>"

COPY bin/azsvcbusbench /
COPY bin/azevhubbench /
COPY bin/azredisbench /
COPY bin/idgen /
COPY bin/ipv4gen /

ENTRYPOINT ["/azsvcbusbench"]
