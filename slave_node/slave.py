import socket
import tqdm
import os
import threading
import time
# device's IP address
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5001
# receive 4096 bytes each time
BUFFER_SIZE = 4096
SEPARATOR = "<SEPARATOR>"
# create the server socket
# TCP socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# bind the socket to our local address
s.bind((SERVER_HOST, SERVER_PORT))
# enabling our server to accept connections
# 5 here is the number of unaccepted connections that
# the system will allow before refusing new connections
s.listen(5)
print(f"[*] Listening as {SERVER_HOST}:{SERVER_PORT}")
# accept connection if there is any
client_socket, address = s.accept() 
# if below code is executed, that means the sender is connected
print(f"[+] {address} is connected.")
# receive the file infos
# receive using client socket, not server socket
received = client_socket.recv(256).decode()
filename, filesize, temp1 = received.split(SEPARATOR)
# remove absolute path if there is
filename = os.path.basename(filename)
# convert to integer
filesize = int(filesize)
with tqdm.tqdm(range(filesize), f"Receiving {filename}", unit="B", unit_scale=True, unit_divisor=1024) as progress:
    with open(filename, "wb") as f:
        while True:
            # read 1024 bytes from the socket (receive)
            bytes_read = client_socket.recv(BUFFER_SIZE)
            if not bytes_read:    
                # nothing is received
                # file transmitting is done
                break
            # write to the file the bytes we just received
            f.write(bytes_read)
            # update the progress bar
            progress.update(len(bytes_read))

# accept connection if there is any
client_socket, address = s.accept() 

from dataGenerator import createDataAndSendDataToServer

runningParameters = temp1.split(',')
#runningParameters= ["10 5", " 5"]
Type = 0
for i in runningParameters:
    for j in i.split():
        threading.Thread(target= createDataAndSendDataToServer, args= (Type, int(j)), daemon=True).start()
    Type += 1

try:
    received = client_socket.recv(256).decode()
    print("command received: " + received + ", end program")
    # close the client socket
    client_socket.close()
    # close the server socket
    s.close()

except KeyboardInterrupt:
    print("Program exited due to KeyboardInterrupt")
    # close the client socket
    client_socket.close()
    # close the socket
    s.close()
    exit(0)
