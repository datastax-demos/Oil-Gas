import socket

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 9999                # Reserve a port for your service.
s.bind(("127.0.0.1", port))        # Bind to the port

s.listen(1) 
c, addr = s.accept()                # Now wait for client connection.
message = ""
while True:
	char = c.recv(1)
	message += char
	if char == "\n":
		print(message)
   		message = ""