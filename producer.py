import random
import time
from datetime import datetime, timedelta
import numpy as np
from json import dumps
import avro
from avro.schema import Parse
from fastavro import writer, reader, schema
from rec_avro import to_rec_avro_destructive, from_rec_avro_destructive, rec_avro_schema
from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder
from kafka import KafkaProducer
import io
import struct


schema = Parse(open('path_to/avro_sch.avsc', "rb").read())

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def sensor_data_generator(sensor_name, val_range, data_datetime, tot_prev=0):
    if (sensor_name == 'Mov1'):
        start_quarter = data_datetime
        random_minute = random.randrange(start_quarter.minute, start_quarter.minute+14)
        random_datetime = data_datetime.replace(minute=random_minute)
        res = {'name': sensor_name, 'timestamp': str(random_datetime), 'value': val_range}
        buf = io.BytesIO()
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([1]))
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(writer_schema=schema)
        writer.write(res, encoder)
        buf.seek(0)
        message_data = (buf.read())
        return message_data, res

    elif (sensor_name == 'Etot'):
        if (data_datetime == datetime(2023, 1, 1, 00, 00, 00)):
            value = tot_prev
        else:
            aug = 2600*24 + float(random.randrange(val_range[0]*100, val_range[1]*100)/100)
            value = tot_prev + aug
        res = {'name': sensor_name, 'timestamp': str(data_datetime), 'value': value}
        buf = io.BytesIO()
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([1]))
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(writer_schema=schema)
        writer.write(res, encoder)
        buf.seek(0)
        message_data = (buf.read())
        return message_data, res

    elif (sensor_name == 'Wtot'):
        if (data_datetime == datetime(2023, 1, 1, 00, 00, 00)):
            value = tot_prev
        else:
            aug = 110 + float(random.randrange(val_range[0]*100, val_range[1]*100)/100)
            value = tot_prev + aug
        res = {'name': sensor_name, 'timestamp': str(data_datetime), 'value': value}
        buf = io.BytesIO()
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([1]))
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(writer_schema=schema)
        writer.write(res, encoder)
        buf.seek(0)
        message_data = (buf.read())
        return message_data, res

    else:
        value = float(random.randrange(val_range[0]*100, val_range[1]*100)/100)
        res = {'name': sensor_name, 'timestamp': str(data_datetime+timedelta(minutes=15)), 'value': value}
        buf = io.BytesIO()
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([1]))
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(writer_schema=schema)
        writer.write(res, encoder)
        buf.seek(0)
        message_data = (buf.read())
        return message_data, res


def w1_late_data_generator(sensor_name, val_range, data_datetime, latency):
    value = float(random.randrange(val_range[0]*100, val_range[1]*100)/100)
    if (latency == 2):
        res = {'name': sensor_name, 'timestamp': str(data_datetime+timedelta(minutes=15)-timedelta(days=latency)), 'value': value}
        buf = io.BytesIO()
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([1]))
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(writer_schema=schema)
        writer.write(res, encoder)
        buf.seek(0)
        message_data = (buf.read())
    if (latency == 10):
        res = {'name': sensor_name, 'timestamp': str(data_datetime+timedelta(minutes=15)-timedelta(days=latency)), 'value': value}
        buf = io.BytesIO()
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([0]))
        buf.write(bytes([1]))
        encoder = BinaryEncoder(buf)
        writer = DatumWriter(writer_schema=schema)
        writer.write(res, encoder)
        buf.seek(0)
        message_data = (buf.read())
    return message_data, res


def generate_random_indexes(array):
    for i in range(5):
        index = random.randrange(0, 95)
        array[index] = 0
    return array


#sensor range of values
TH1_range = TH2_range = [12, 35]
HVAC1_range = [0, 100]
HVAC2_range = [0, 200]
MiAC1_range = [0, 150]
MiAC2_range = [0, 200]
W1_range = [0, 1]
Etot_range = [-1000, 1000]
Wtot_range = [-10, 10]

data_datetime = datetime(2020, 1, 1, 00, 00, 00)
ones = np.ones(96)

count = 0
sec = 0
random_indexes = generate_random_indexes(ones)

etotprev = 1500.8
wtotprev = 850.8

schema_id = 1
schema_id_bytes = struct.pack('>I', schema_id)

while True:
    count = count + 1
    sec = sec + 1

    data_TH1, res_th1 = sensor_data_generator('TH1', TH1_range, data_datetime)
    data_TH2, res_th2 = sensor_data_generator('TH2', TH2_range, data_datetime)
    data_HVAC1, res_hvac1 = sensor_data_generator('HVAC1', HVAC1_range, data_datetime)
    data_HVAC2, res_hvac2 = sensor_data_generator('HVAC2', HVAC2_range, data_datetime)
    data_MiAC1, res_miac1 = sensor_data_generator('MiAC1', MiAC1_range, data_datetime)
    data_MiAC2, res_miac2 = sensor_data_generator('MiAC2', MiAC2_range, data_datetime)
    data_W1, res_w1 = sensor_data_generator('W1', W1_range, data_datetime)
    data_Mov1, res_mov1 = sensor_data_generator('Mov1', 1, data_datetime)

    producer.send('TH1', value=data_TH1)
    producer.send('TH2', value=data_TH2)
    producer.send('HVAC1', value=data_HVAC1)
    producer.send('HVAC2', value=data_HVAC2)
    producer.send('MiAC1', value=data_MiAC1)
    producer.send('MiAC2', value=data_MiAC2)
    producer.send('W1', value=data_W1)

    if (count == 1):
        data_Etot, res_etot = sensor_data_generator('Etot', Etot_range, data_datetime, etotprev)
        producer.send('Etot', value=data_Etot)
        etotprev = res_etot['value']

        data_Wtot, res_wtot = sensor_data_generator('Wtot', Wtot_range, data_datetime, wtotprev)
        producer.send('Wtot', value=data_Wtot)
        wtotprev = res_wtot['value']

    if (random_indexes[count-1] == 0):
        producer.send('Mov1', value=data_Mov1)

    if(count == 96):
        count = 0
        ones = np.ones(96)
        random_indexes = generate_random_indexes(ones)

    if(sec % 20 == 0):
        late_data_W1_two, res_w1two = w1_late_data_generator('W1', W1_range, data_datetime, 2)
        producer.send('W1', value=late_data_W1_two)

    if(sec % 120 == 0):
        late_data_W1_ten, res_w1ten = w1_late_data_generator('W1', W1_range, data_datetime, 10)
        producer.send('W1', value=late_data_W1_ten)


    time.sleep(1)
    data_datetime = data_datetime + timedelta(minutes=15)
