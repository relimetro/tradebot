
[ $1 == "test" ] && { curl -X POST -d '{"test":"Bob"}' http://localhost:80/v1/test; exit; }
[ $1 == "pred" ] && { curl -X POST -d '{"test":"Bob"}' http://localhost:80/v1/GetPrediction	; exit; }
[ $1 == "feat" ] && { curl -X POST -d '{"test":"Bob"}' http://localhost:80/v1/GetFeatures	; exit; }
