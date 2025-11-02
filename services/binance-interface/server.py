import os
import grpc
from concurrent import futures
from grpc_reflection.v1alpha import reflection
from binance.client import Client
from binance.exceptions import BinanceAPIException

import binance_interface_pb2
import binance_interface_pb2_grpc


BINANCE_CLIENT = None


def get_binance_client():
    global BINANCE_CLIENT
    if BINANCE_CLIENT is None:
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")

        if not api_key or not api_secret:
            raise RuntimeError("Missing Binance API credentials in environment variables.")

        BINANCE_CLIENT = Client(api_key, api_secret)
        print("Binance client initialized.")
    return BINANCE_CLIENT


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
    def __init__(self):
        self.client = get_binance_client()

    def GetKlines(self, request, context):
        print(f"[GetKlines] symbol={request.symbol}, interval={request.interval}, "
              f"start={request.start_time}, end={request.end_time}")

        try:
            klines = self.client.get_klines(
                symbol=request.symbol,
                interval=request.interval,
                startTime=request.start_time if request.start_time else None,
                endTime=request.end_time if request.end_time else None,
            )

            kline_msgs = [
                Kline(
                    open_time=k[0],
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                    close_time=k[6],
                    quote_asset_volume=float(k[7]),
                    number_of_trades=int(k[8]),
                    taker_buy_base_volume=float(k[9]),
                    taker_buy_quote_volume=float(k[10])
                ).to_proto()
                for k in klines
            ]

            return binance_interface_pb2.GetKlinesResponse(klines=kline_msgs)

        except BinanceAPIException as e:
            context.set_details(f"Binance API error: {e.message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return binance_interface_pb2.GetKlinesResponse()

    def GetSingleKline(self, request, context):
        print(f"[GetSingleKline] symbol={request.symbol}, interval={request.interval}, "
              f"timestamp={request.timestamp}")

        try:
            klines = self.client.get_klines(
                symbol=request.symbol,
                interval=request.interval,
                limit=1,
                endTime=request.timestamp if request.timestamp else None
            )

            if not klines:
                return binance_interface_pb2.GetSingleKlineResponse()

            k = klines[0]
            kline_obj = Kline(
                open_time=k[0],
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                close_time=k[6],
                quote_asset_volume=float(k[7]),
                number_of_trades=int(k[8]),
                taker_buy_base_volume=float(k[9]),
                taker_buy_quote_volume=float(k[10])
            )

            return binance_interface_pb2.GetSingleKlineResponse(kline=kline_obj.to_proto())

        except BinanceAPIException as e:
            context.set_details(f"Binance API error: {e.message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return binance_interface_pb2.GetSingleKlineResponse()


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
    print("Binance interface running on port 50052")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

