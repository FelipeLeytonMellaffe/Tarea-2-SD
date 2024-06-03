import json
import logging
import smtplib
from email.mime.text import MIMEText
from flask import Flask, request, jsonify
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'topic_notificaciones', 
    bootstrap_servers='kafka:9092', 
    group_id='consumer_group_notificaciones', 
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
estado_pedidos = {}

# Configura tu servidor SMTP aquí
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "tareasd2@gmail.com"
SMTP_PASSWORD = "anna kiaj dfnc ghtl"

def send_email(to, subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = SMTP_USERNAME
    msg['To'] = to

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SMTP_USERNAME, to, msg.as_string())
    #logger.info(f"Correo enviado a {to}: {subject} - {body}")

@app.route('/pedido/<id>', methods=['GET'])
def consulta_pedido(id):
    pedido = estado_pedidos.get(id)
    if pedido:
        return jsonify(pedido), 200
    else:
        return jsonify({'error': 'Pedido no encontrado'}), 404

def consumir_mensajes():
    for message in consumer:
        pedido = message.value
        estado_pedidos[pedido['id']] = pedido
        logger.info(f"Actualización de pedido: {pedido}")
        send_email(pedido['correo'], 'Actualización de Pedido', f"ID: {pedido['id']}, Nombre: {pedido['nombre']}, Precio: {pedido['precio']}, Estado: {pedido['estado']}")

if __name__ == '__main__':
    executor = ThreadPoolExecutor(max_workers=4)
    executor.submit(consumir_mensajes)
    app.run(host='0.0.0.0', port=5001)

