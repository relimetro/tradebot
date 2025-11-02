# tradebot

# build and run
```
sudo docker compose -p tradebot up --build -d
```

**Test**

`grpcurl -plaintext localhost:50052 list`

```
grpcurl -plaintext -d '{ "symbol": "BTCUSDT", "interval": "1m", "start_time": 1728000000000, "end_time": 1728086400000 }' localhost:50052 binance_interface.BinanceData.GetKlines
grpcurl -plaintext -d '{ "symbol": "ETHUSDT", "interval": "5m", "timestamp": 1728086400000 }' localhost:50052 binance_interface.BinanceData.GetSingleKline
```
