attempt_counter=0
max_attempts=10

until(curl --location -u "elastic:password" --request PUT 'http://localhost:9200/_snapshot/backup' \
--header 'Content-Type: application/json' \
--data-raw '{
  "type": "fs",
  "settings": {
    "location": "/mount/backups/backup"
  }
}'); do
    if [ ${attempt_counter} -eq ${max_attempts} ];then
        echo "Max attempts reached"
        exit 1
        fi

        printf '.'
        attempt_counter=$(($attempt_counter+1))
        sleep 5
done

attempt_counter=0

until(curl  --location -u "elastic:password"  --request POST 'http://localhost:9200/_snapshot/backup/snapshot_1/_restore' \
--data-raw ''); do
    if [ ${attempt_counter} -eq ${max_attempts} ];then
        echo "Max attempts reached"
        exit 1
        fi

        printf '.'
        attempt_counter=$(($attempt_counter+1))
        sleep 5
done
