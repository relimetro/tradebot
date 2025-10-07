import grpc
from concurrent import futures
import dummy_pb2
import dummy_pb2_grpc
from grpc_reflection.v1alpha import reflection

class HelloService(dummy_pb2_grpc.HelloServiceServicer):
    def SayHello(self, request, context):
        return dummy_pb2.HelloReply(message=f"Hello, {request.name}!")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dummy_pb2_grpc.add_HelloServiceServicer_to_server(HelloService(), server)

    SERVICE_NAMES = (
        dummy_pb2_grpc.HelloServiceServicer.__name__,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port('[::]:50051')
    server.start()
    print("HelloService running on port 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()

