import json
import uuid
import logging
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers='kafka:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/pedido', methods=['POST'])
def pedido():
    data = request.json
    data['id'] = str(uuid.uuid4())  # Genera un ID único
    data['estado'] = 'recibido'
    producer.send('topic_pedidos', data)
    logger.info(f"Pedido recibido: {data}")
    return jsonify(data), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
