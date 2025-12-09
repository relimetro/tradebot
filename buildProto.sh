
GOOGLEAPIS_DIR="./googleapis"

python -m grpc_tools.protoc \
	-I${GOOGLEAPIS_DIR} -I. --include_imports --include_source_info --descriptor_set_out=services/controller/controller.pb \
	--proto_path=./proto/ \
	--python_out=./services/controller \
	--grpc_python_out=./services/controller \
	controller.proto


