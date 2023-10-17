seed-db:
	docker compose up -d postgres;
	echo "Waiting 5 seconds for service to start...";
	sleep 5;
	PGPASSWORD=testlab01 \
	psql -U postgres -h localhost -d nyt_covid19 -a -f ./seed/schema_data.sql
build:
	for service in etl-master etl-worker; do \
		docker build --no-cache -t distributed-etl-app/$$service:development ./$$service ; \
	done
up:
	docker compose up -d
submit:
	docker exec etl-worker-1 \
		/usr/local/spark/bin/spark-submit \
		--master spark://etl-master:7077 \
		$(app_path)
down:
	docker compose down