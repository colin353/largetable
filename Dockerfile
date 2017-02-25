FROM scratch
MAINTAINER Colin Merkel <colinmerkel@podkarma.com>

# Add the application binary.
ADD target/release/largetable /

# Add the supplementary files.
ADD config /config
ADD data /data

CMD ["/largetable"]

EXPOSE 8080
