FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
COPY scaler.pkl .
COPY xgb_model.pkl .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]

