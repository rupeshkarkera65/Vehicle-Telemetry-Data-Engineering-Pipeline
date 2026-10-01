import logging
import os
from azure.functions import KafkaEvent

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from twilio.rest import Client
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

import json
from datetime import datetime

# -----------------------------
# Schema Registry
# -----------------------------
sr_conf = {
    "url": os.getenv("SCHEMA_REGISTRY_URL"),
    "basic.auth.user.info": (
        f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:"
        f"{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    )
}

schema_registry_client = SchemaRegistryClient(sr_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

# -----------------------------
# Twilio Client
# -----------------------------
twilio_client = Client(
    os.getenv("TWILIO_ACCOUNT_SID"),
    os.getenv("TWILIO_AUTH_TOKEN")
)

# -----------------------------
# Vehicle → Driver Mapping
# -----------------------------
VEHICLE_PHONE_MAP = {
    "VEH_001": "your_phone_number",
    "VEH_002": "your_phone_number",
    "VEH_003": "your_phone_number",
    "VEH_004": "your_phone_number",
    "VEH_005": "your_phone_number",
    "VEH_006": "your_phone_number",
    "VEH_007": "your_phone_number",
    "VEH_008": "your_phone_number",
    "VEH_009": "your_phone_number",
    "VEH_010": "your_phone_number"
}

# -----------------------------
# Blob Storage Client
# -----------------------------
blob_service_client = BlobServiceClient.from_connection_string(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
)

CONTAINER_NAME = "landing"

# -----------------------------
# Azure Function Entry Point
# -----------------------------
def main(event: KafkaEvent) -> None:
    """
    Process vehicle telemetry from Kafka and store in Azure Blob Storage.
    Send SMS alerts for critical conditions.
    """
    try:
        logging.info("=== Processing Kafka Event ===")
        
        # Deserialize Avro message
        raw_bytes = event.get_body()
        decoded_value = avro_deserializer(
            raw_bytes,
            SerializationContext(event.topic, MessageField.VALUE)
        )

        logging.info(f"Decoded message for vehicle: {decoded_value.get('vehicle_id')}")

        # Extract telemetry data
        vehicle_id = decoded_value.get("vehicle_id")
        speed = decoded_value.get("speed_kmph")
        temperature = decoded_value.get("temperature_c")
        fuel_level = decoded_value.get("fuel_percent")
        latitude = decoded_value.get("latitude")
        longitude = decoded_value.get("longitude")
        timestamp = decoded_value.get("timestamp")

        # Validate required fields
        if not all([vehicle_id, speed, temperature, fuel_level]):
            logging.warning("Missing mandatory telemetry fields")
            return

        # -----------------------------
        # Write to Azure Blob Storage
        # -----------------------------
        now = datetime.utcnow()
        date_folder = now.strftime("%Y-%m-%d")
        blob_name = f"vehicle_telemetry/{date_folder}.jsonl"

        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_name
        )

        # Prepare payload with Kafka metadata
        payload = {
            "kafka_metadata": {
                "topic": event.topic,
                "partition": event.partition,
                "offset": event.offset,
                "timestamp": event.timestamp
            },
            "telemetry": decoded_value,
            "ingestion_time": now.isoformat()
        }

        data = json.dumps(payload) + "\n"

        # Create append blob if it doesn't exist
        try:
            blob_client.get_blob_properties()
        except ResourceNotFoundError:
            blob_client.create_append_blob()
            logging.info(f"Created new append blob: {blob_name}")

        # Append data to blob
        blob_client.append_block(data)
        logging.info(f"✅ Appended telemetry to: {blob_name}")

        # -----------------------------
        # Send Alert if Threshold Exceeded
        # -----------------------------
        alert_triggered = False
        alert_reasons = []

        if speed > 80:
            alert_triggered = True
            alert_reasons.append(f"High speed: {speed} km/h")
        
        if temperature > 90:
            alert_triggered = True
            alert_reasons.append(f"High temperature: {temperature}°C")
        
        if fuel_level < 10:
            alert_triggered = True
            alert_reasons.append(f"Low fuel: {fuel_level}%")

        if alert_triggered:
            to_number = VEHICLE_PHONE_MAP.get(vehicle_id)

            if not to_number:
                logging.warning(f"No phone number mapped for vehicle {vehicle_id}")
            else:
                try:
                    message_body = (
                        f"🚨 Vehicle Alert - {vehicle_id}\n\n"
                        f"Alerts:\n"
                        f"{chr(10).join(['• ' + reason for reason in alert_reasons])}\n\n"
                    )

                    # Send SMS
                    msg = twilio_client.messages.create(
                        body=message_body,
                        from_=os.getenv("TWILIO_FROM_NUMBER"),
                        to=to_number
                    )
                    
                    logging.info(f"✅ SMS alert sent: {msg}")

                
                except Exception as twilio_error:
                    logging.error(f"Failed to send alert: {twilio_error}")


        logging.info("=== Event Processing Completed ===")

    except Exception as e:
        logging.error(f"❌ Error processing Kafka message: {str(e)}", exc_info=True)
        raise