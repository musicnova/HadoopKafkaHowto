# Dockerfile
FROM ubuntu:16.04
ARG UID="1000"
ENV EDITOR="/bin/nano"
RUN apt -yqq update && apt -yqq install nano
RUN useradd -m -u $UID m1user
USER m1user
RUN touch /home/m1user/test
RUN chown -R m1user /home/m1user/test
RUN chmod go+w /home/m1user/test
VOLUME /home/m1user
WORKDIR /home/m1user
ENTRYPOINT exec /bin/sh
LABEL maintainer="M1User"
