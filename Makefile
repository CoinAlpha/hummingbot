.ONESHELL:
.PHONY: test
.PHONY: run_coverage
.PHONY: report_coverage
.PHONY: development-diff-cover

RPC_PROTO_DIR = hummingbot/client/controller/rpc
RPC_PROTO_TARGET = $(RPC_PROTO_DIR)/*.proto
RPC_PROTO_OUT = hummingbot/client/controller/rpc

test:
	coverage run -m nose --exclude-dir="test/connector" --exclude-dir="test/debug" --exclude-dir="test/mock"

run_coverage: test
	coverage report
	coverage html

report_coverage:
	coverage report
	coverage html

development-diff-cover:
	coverage xml
	diff-cover --compare-branch=origin/development coverage.xml

rpc-protos:
	python -m grpc_tools.protoc -I$(RPC_PROTO_DIR) --python_out=$(RPC_PROTO_OUT) --grpc_python_out=$(RPC_PROTO_OUT) $(RPC_PROTO_TARGET)
