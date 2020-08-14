FROM elasticsearch:6.8.11

RUN echo 'path.repo: ["/mount/backups"]' >> /usr/share/elasticsearch/config/elasticsearch.yml
COPY tests/snapshot/backup/ /mount/backups/backup/
RUN chmod -R 777 /mount/backups/