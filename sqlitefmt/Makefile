.PHONY: inspect
inspect:
	go run cmd/inspect/main.go dbs/test.db

.PHONY: debug
debug:
	dlv debug cmd/inspect/main.go -- dbs/test.db

