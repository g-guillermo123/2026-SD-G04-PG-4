.PHONY: build run stop clean logs status

build:
	docker-compose build

run:
	docker-compose up -d

stop:
	docker-compose down

clean:
	docker-compose down -v
	docker system prune -f

logs:
	docker-compose logs -f

status:
	@for port in 8001 8002 8003 8004 8005; do \
		echo "=== Nodo en puerto $$port ==="; \
		curl -s http://localhost:$$port/estado || echo "No responde"; \
	done
