FROM scylladb/scylla:latest

COPY schema.cql /schema_creator.cql
COPY init-db.sh /init-db.sh
RUN chmod +x /init-db.sh

ENTRYPOINT ["/init-db.sh"]
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/scylla-server.conf"]