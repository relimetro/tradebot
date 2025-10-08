import grpc
from concurrent import futures
import binance_interface_pb2
import binance_interface_pb2_grpc
from grpc_reflection.v1alpha import reflection

class Kline:
    def __init__(self, open_time=0, open=0.0, high=0.0, low=0.0, close=0.0,
                 volume=0.0, close_time=0, quote_asset_volume=0.0,
                 number_of_trades=0, taker_buy_base_volume=0.0, taker_buy_quote_volume=0.0):
        self.open_time = open_time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.close_time = close_time
        self.quote_asset_volume = quote_asset_volume
        self.number_of_trades = number_of_trades
        self.taker_buy_base_volume = taker_buy_base_volume
        self.taker_buy_quote_volume = taker_buy_quote_volume

    def to_proto(self):
        """Convert this class to a protobuf Kline message"""
        return binance_interface_pb2.Kline(
            open_time=self.open_time,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            close_time=self.close_time,
            quote_asset_volume=self.quote_asset_volume,
            number_of_trades=self.number_of_trades,
            taker_buy_base_volume=self.taker_buy_base_volume,
            taker_buy_quote_volume=self.taker_buy_quote_volume,
        )

class BinanceData(binance_interface_pb2_grpc.BinanceDataServicer):
    def GetKlines(self, request, context):
        print(f"Received GetKlines request for {request.symbol} from {request.start_time} to {request.end_time}")
        empty_kline = Kline().to_proto()
        return binance_interface_pb2.GetKlinesResponse(klines=[empty_kline])

    def GetSingleKline(self, request, context):
        print(f"Received GetSingleKline request for {request.symbol} interval {request.interval}")
        empty_kline = Kline().to_proto()
        return binance_interface_pb2.GetSingleKlineResponse(kline=empty_kline)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    binance_interface_pb2_grpc.add_BinanceDataServicer_to_server(BinanceData(), server)

    SERVICE_NAMES = (
        binance_interface_pb2.DESCRIPTOR.services_by_name['BinanceData'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port('[::]:50052')
    server.start()
    print("binance-interface running on port 50052")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

