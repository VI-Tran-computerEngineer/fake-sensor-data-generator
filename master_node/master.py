import socket
import tqdm
import threading
from dataGenerator import createDataAndSendDataToServer


if __name__ == '__main__':
    SEPARATOR = "<SEPARATOR>"
    BUFFER_SIZE = 4096 # send 4096 bytes each time step
    # the ip address or hostname of the server, the receiver
    host = input("input IPv4 of the host")
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

