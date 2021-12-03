import math
import datetime
from numpy import random
import sys
import os
import time
import socket
import tqdm
import threading

from faker import Faker
from faker.providers import BaseProvider
from confluent_kafka import Producer

fake = Faker()

# first, import a similar Provider or use the default one

denta_H_vap = 40660 # (J/mol)
R = 8.314 # J/(mol*K)
sampleRate = 1 # one sample/minute
runningParameters =[[10, 5], [10], [10]]

# create new provider class
class MyProvider(BaseProvider):
    # average temperature
    def temperature(self, duration, offset = 0) -> float:
        maxTemp= 40 # Maximum temperature
        minTemp= 25 # Minimum temperature
        minutesInOneDay = 24*60;
        u_0AM = 26.5 # mean of temperature
        denta_0AM = 0.1 # standard deviation
        u_changeAmount = (maxTemp - minTemp) / (minutesInOneDay)


        date = str(datetime.datetime.now())
        hour = date[11:16]
        jump = int(hour[0:2])*60 + int(hour[3:5]) + offset
        if jump <= 180:
            u = u_0AM + (-jump)*u_changeAmount
        elif jump <= 720:
            u = u_0AM + (jump-180)*u_changeAmount
        else:
            u = u_0AM + (720*2-180-jump)*u_changeAmount

        dataImediateSum = 0

        for i in range(int(duration/sampleRate) + 1):
            dataImediateSum += random.normal(u, denta_0AM)
            jump += 1
            if jump <= 180 :
                u -= u_changeAmount
            elif jump <= 720:
                u += u_changeAmount
            else:
                u -= u_changeAmount

        data = dataImediateSum / (duration/sampleRate+1)
        return data

    def huminidy(self, duration) -> float:
        refTemperature = 27 # reference temperature in TP HCM at 10h15 03/11/2021 (C)
        refVapourPresure = 83 # reference vapour pressure in TP HCM at 10h15 03/11/2021 corresponding to the temperature above(%)

        dataImediateSum = 0
        for i in range(int(duration/sampleRate) + 1):
            currentTemperature = self.temperature(0, i)
            currentVapour = refVapourPresure / math.exp(
                (denta_H_vap / R) * (1 / (currentTemperature+273) - 1 / (refTemperature+273)))
            #print(currentVapour)
            dataImediateSum += currentVapour

        data = dataImediateSum/(duration/sampleRate + 1)
        return data

    def lightIntensity(self, duration) -> float:
        dentaTime = 2*60
        refLightIntensity = 4000 # (lux)
        refHour = 12*60 # (hour)

        date = str(datetime.datetime.now())
        hour = date[11:16]

        u_refLightIntensityRelative = 1/(dentaTime*math.sqrt(2*math.pi))

        dataImediateSum = 0
        for i in range(int(duration / sampleRate) + 1):
            jump = abs(12 * 60 - (int(hour[0:2]) * 60 + int(hour[3:5]) + i))
            u_currentLightIntensityRelative = 1 / (dentaTime * math.sqrt(2 * math.pi)) * math.exp(
                -0.5 * pow(jump / dentaTime, 2))
            u_currentLightIntensity = u_currentLightIntensityRelative/u_refLightIntensityRelative * refLightIntensity
            dentaLight = u_currentLightIntensity/50
            dataImediateSum += random.normal(u_currentLightIntensity, dentaLight)


        data = dataImediateSum / (duration / sampleRate + 1)
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
    

if __name__ == '__main__':
    SEPARATOR = "<SEPARATOR>"
    BUFFER_SIZE = 4096 # send 4096 bytes each time step
    # the ip address or hostname of the server, the receiver
    host = "192.168.12.105"
    # the port, let's use 5001
    port = 5001
    # the name of file we want to send, make sure it exists
    filename = "dataGenerator.py"
    # get the file size
    filesize = os.path.getsize(filename)
    # create the client socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"[+] Connecting to {host}:{port}")
    s.connect((host, port))
    print("[+] Connected.")
    # send the filename and filesize
    s.send(f"{filename}{SEPARATOR}{filesize}{SEPARATOR}10 5, 5,".encode())
    # start sending the file
    with tqdm.tqdm(range(filesize), f"Sending {filename}", unit="B", unit_scale=True, unit_divisor=1024) as progress:
        with open(filename, "rb") as f:
            while True:
                # read the bytes from the file
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    # file transmitting is done
                    break
                # we use sendall to assure transimission in 
                # busy networks
                s.sendall(bytes_read)
                # update the progress bar
                progress.update(len(bytes_read))

    # close the socket
    s.close()
    # create the client socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    runningParameters =[[10, 5], [10], [10]]
    for i in range(len(runningParameters)):
        for j in runningParameters[i]:
            threading.Thread(target= createDataAndSendDataToServer, args= (i, j), daemon=True).start()

    try:
        while True:
            userInput = input("press \"stop\" if you want to stop the program: ")
            if (userInput == "stop"):
                print("Program exited due to stop command")
                s.send("stop".encode())
                # close the socket
                s.close()
                exit(0)
            else:
                print("Such command is not exist")
    except KeyboardInterrupt:
        print("Program exited due to KeyboardInterrupt")
        s.send("stop".encode())
        # close the socket
        s.close()
        exit(0)

