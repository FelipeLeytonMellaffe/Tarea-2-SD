import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from concurrent.futures import ThreadPoolExecutor

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'topic_pedidos', 
    bootstrap_servers='kafka:9092', 
    group_id='consumer_group_procesamiento', 
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='kafka:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

estados = ['recibido', 'preparando', 'entregando', 'finalizado']

# Variables globales para el tiempo de procesamiento
pending_pedidos = 0

def procesar_pedido(pedido):
    global pending_pedidos

    for estado in estados:
        pedido['estado'] = estado
        producer.send('topic_procesamiento', pedido)
        producer.send('topic_notificaciones', pedido)
        time.sleep(5)  # Simulación del tiempo de procesamiento

    logger.info(f"Pedido procesado: {pedido['id']}")

    pending_pedidos -= 1
    if pending_pedidos == 0:
        logger.info(f"Todos los pedidos han sido procesados")

def consumir_mensajes():
    global pending_pedidos

    for message in consumer:
        pedido = message.value
        logger.info(f"Recibiendo pedido: {pedido['id']}")
        pending_pedidos += 1
        executor.submit(procesar_pedido, pedido)

if __name__ == '__main__':
    executor = ThreadPoolExecutor(max_workers=4)  # Número de subprocesos
    consumir_mensajes()
