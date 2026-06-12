.PHONY: run stop clean
run:
	docker compose up --build -d
stop:
	docker compose down
clean:
	docker compose down -v
	rm -f db_data_*.json data/*.csv
