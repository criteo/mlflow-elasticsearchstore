curl --location -u "elastic:password" --request PUT 'http://127.0.0.1:9200/_snapshot/my_fs_backup' \
--header 'Content-Type: application/json' \
--data-raw '{
  "type": "fs",
  "settings": {
    "location": "/mount/backups/my_fs_backup_location"
  }
}'

curl --location -u "elastic:password"  --request POST 'http://127.0.0.1:9200/_snapshot/my_fs_backup/snapshot_1/_restore' \
--data-raw ''