import os
import grpc
from concurrent import futures
import requests
from grpc_reflection.v1alpha import reflection
import datetime
import traceback

from pymongo import MongoClient

import controller_pb2
import controller_pb2_grpc


feature_cols = ["High", "Open", "Close", "avg_true_range_14", "momentum_48h", "rolling_return_mean_24", "BB_width", "ema_24", "MACD", "BB_middle"]

class Controller(controller_pb2_grpc.ControllerServicer):
	def Test(self, request, context):
		print("Test",request)
		return controller_pb2.TestResponse(
			out=f"Test {request.test}")

	def GetPrediction(self,request,context):
		try:
			print("get prediction")
			with MongoClient("mongodb://mongo:27017") as mongo:
				db = mongo["market_data"]
				pred = db["predictions"]

				latest = None
				val = 0

				for x in pred.find():
					## dt = datetime.date(x["datetime"].split("T")[0])
					dt = x["datetime"]
					print(type(dt))
					v = x["prediction"]
					if latest == None or dt > latest :
						latest = dt
						val = v
				print(val)
				return controller_pb2.PredictionResponse(
					prediction=f"{val}")
		except Exception as e : print(e)


	def GetFeatures(self,request,context):
		try:
			print("get features")
			with MongoClient("mongodb://mongo:27017") as mongo:
				db = mongo["market_data"]
				klines = db["klines"]

				# out = []
				out = controller_pb2.FeaturesResponse()

				print("start")
				for x in klines.find(limit=10):
					out.data.add(
							High = str(x["High"]),
							Open = str(x["Open"]),
							Close = str(x["Close"]),
							avg_true_range_14 = str(x["avg_true_range_14"]),
							momentum_48h = str(x["momentum_48h"]),
							rolling_return_mean_24 = str(x["rolling_return_mean_24"]),
							BB_width = str(x["BB_width"]),
							ema_24 = str(x["ema_24"]),
							MACD = str(x["MACD"]),
							BB_middle = str(x["BB_middle"])
							)
				print("done")
				return out
				# return controller_pb2.FeaturesResponse(
					# data=out)
		except Exception as e :
			print(e)
			traceback.print_exec()





def main():
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	controller_pb2_grpc.add_ControllerServicer_to_server(Controller(), server)

	SERVICE_NAMES = (
		controller_pb2.DESCRIPTOR.services_by_name["Controller"].full_name,
		reflection.SERVICE_NAME,
	)
	reflection.enable_server_reflection(SERVICE_NAMES, server)

	server.add_insecure_port("[::]:50061")
	server.start()
	print("serving port 50061")
	server.wait_for_termination()



if __name__ == "__main__": main()

