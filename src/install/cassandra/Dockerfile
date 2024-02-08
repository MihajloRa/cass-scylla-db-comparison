FROM cassandra:3.11.2

ENV LOCAL_JMX no

# Update cassandra-env.sh to set RMI server hostname and custom JMX port
RUN sed -i 's/# JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=$JMX_HOST"/JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=${JMX_HOST:-localhost}"/g' /etc/cassandra/cassandra-env.sh \
    && echo 'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"' >> /etc/cassandra/cassandra-env.sh \
    && echo 'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false"' >> /etc/cassandra/cassandra-env.sh
