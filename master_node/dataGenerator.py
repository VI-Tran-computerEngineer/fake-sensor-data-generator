import math
import datetime
from numpy import random
import sys
import os
import time

from faker import Faker
from faker.providers import BaseProvider
from confluent_kafka import Producer

fake = Faker()

# first, import a similar Provider or use the default one

denta_H_vap = 40660 # (J/mol)
R = 8.314 # J/(mol*K)
samplePeriod = 1 # 1 minute/sample

# create new provider class
class MyProvider(BaseProvider):
    # average temperature
    def temperature(self, duration, offset = 0) -> float:
        maxTemp= 40 # Maximum temperature at 12:00 AM
        minTemp= 25 # Minimum temperature at 03:00 AM
        u_03AM = minTemp # mean of temperature at 00:00 AM
        temperature_denta = 0.1 #  standard deviation of temperature
        u_changeAmountFrom3AMTo12noon = (maxTemp - minTemp) / (9*60) # > 0
        u_changeAmountFrom12noonTo3AM = (minTemp - maxTemp) / (15*60) # < 0

        date = str(datetime.datetime.now())
        hour = date[11:16]
        jump = int(hour[0:2])*60 + int(hour[3:5]) + offset - 180
        if jump < 0:
            jump += 24*60

        if jump <= 9*60:
            u = u_03AM + jump*u_changeAmountFrom3AMTo12noon
        else:
            u = maxTemp + (jump - 9*60)*u_changeAmountFrom12noonTo3AM

        dataImediateSum = 0

        for i in range(int(duration/samplePeriod) + 1):
            dataImediateSum += random.normal(u, temperature_denta)
            jump += samplePeriod
            if jump <= 9*60:
                u += u_changeAmountFrom3AMTo12noon
            else:
                u += u_changeAmountFrom12noonTo3AM

        data = dataImediateSum / (duration/samplePeriod+1)
        return data

    def huminidy(self, duration) -> float:
        refTemperature = 27 # reference temperature in TP HCM at 10h15 03/11/2021 (C)
        refVapourPresure = 83 # reference vapour pressure in TP HCM at 10h15 03/11/2021 corresponding to the temperature above(%)

        dataImediateSum = 0
        for i in range(int(duration/samplePeriod) + 1):
            currentTemperature = self.temperature(0, i*samplePeriod)
            currentVapour = refVapourPresure / math.exp(
                (denta_H_vap / R) * (1 / (currentTemperature+273) - 1 / (refTemperature+273)))
            
            dataImediateSum += currentVapour

        data = dataImediateSum/(duration/samplePeriod + 1)
        return data

    def lightIntensity(self, duration) -> float:
        dentaTime = 2*60
        refLightIntensity = 4000 # (lux)
        refHour = 12*60 # (hour)

        date = str(datetime.datetime.now())
        hour = date[11:16]

        u_refLightIntensityRelative = 1/(dentaTime*math.sqrt(2*math.pi))

        dataImediateSum = 0
        for i in range(int(duration / samplePeriod) + 1):
            jump = abs(12 * 60 - (int(hour[0:2]) * 60 + int(hour[3:5]) + i))
            u_currentLightIntensityRelative = 1 / (dentaTime * math.sqrt(2 * math.pi)) * math.exp(
                -0.5 * pow(jump / dentaTime, 2))
            u_currentLightIntensity = u_currentLightIntensityRelative/u_refLightIntensityRelative * refLightIntensity
            dentaLight = u_currentLightIntensity/50
            dataImediateSum += random.normal(u_currentLightIntensity, dentaLight)


        data = dataImediateSum / (duration / samplePeriod + 1)
        return data

def createDataAndSendDataToServer(Type, duration):
    topic = os.environ['CLOUDKARAFKA_TOPIC'].split(",")[0]

    # Consumer configuration
    conf = {
        'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
    }

    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))

    fake.add_provider(MyProvider)
    while True:
        time.sleep(20)
        if Type == 0:
            msg = str(fake.temperature(duration))
        elif Type == 1:
            msg = str(fake.huminidy(duration))
        else:
            msg = str(fake.lightIntensity(duration))
        try:
            p.produce(topic, msg, partition=Type)#callback=delivery_callback, partition=Type)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()