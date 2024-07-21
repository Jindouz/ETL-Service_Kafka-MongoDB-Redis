FROM bitnami/zookeeper:latest


EXPOSE 2181

ENV ALLOW_ANONYMOUS_LOGIN=yes
ENV ZOOKEEPER_SERVER_ID=1

CMD ["zkServer.sh", "start-foreground"]
