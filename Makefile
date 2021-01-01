APP=tezos_index
build: clean
	go build -o ${APP}

run:
	go run -race
clean:
	go clean