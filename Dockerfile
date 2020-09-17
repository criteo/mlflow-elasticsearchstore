FROM elasticsearch:7.7.0

RUN echo 'path.repo: ["/mount/backups"]' >> /usr/share/elasticsearch/config/elasticsearch.yml
COPY tests/snapshot/backup/ /mount/backups/backup/
RUN chmod -R 777 /mount/backups/