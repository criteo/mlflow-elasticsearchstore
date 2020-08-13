FROM elasticsearch:6.8.11

RUN echo 'path.repo: ["/mount/backups"]' >> /usr/share/elasticsearch/config/elasticsearch.yml
COPY snapshots/my_fs_backup_location/ /mount/backups/my_fs_backup_location/
RUN chmod -R 777 /mount/backups/
RUN sudo sysctl -w vm.max_map_count=262144