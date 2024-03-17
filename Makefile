BINARY_NAME=golang_bigtable_example

setup-db:
	export BIGTABLE_EMULATOR_HOST="localhost:8086" BIGTABLE_PROJECT_ID="local" BIGTABLE_INSTANCE_ID="local-instance" && \
	cbt -project local -instance local-instance createtable media_progress && \
	cbt -project local -instance local-instance createfamily media_progress data && \
	cbt -project local -instance local-instance createtable last_by_title && \
	cbt -project local -instance local-instance createfamily last_by_title data && \
	cbt -project local -instance local-instance createtable last_by_user && \
	cbt -project local -instance local-instance createfamily last_by_user data

cleanup-db:
	export BIGTABLE_EMULATOR_HOST="localhost:8086" BIGTABLE_PROJECT_ID="local" BIGTABLE_INSTANCE_ID="local-instance" && \
	cbt -project local -instance local-instance deleteallrows media_progress && \
	cbt -project local -instance local-instance deleteallrows last_by_title && \
	cbt -project local -instance local-instance deleteallrows last_by_user

build:
	@go build -v -o ${BINARY_NAME} .

insert-one: build
	@./${BINARY_NAME} insert one

insert-conditional: build
	@./${BINARY_NAME} insert conditional

insert-batch: build
	@./${BINARY_NAME} insert batch

read-one: build
	@./${BINARY_NAME} read one

read-multiple: build
	@./${BINARY_NAME} read multiple

read-partialKey: build
	@./${BINARY_NAME} read partialKey

delete: build
	@./${BINARY_NAME} delete
