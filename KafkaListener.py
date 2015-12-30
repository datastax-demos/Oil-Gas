from kafka import SimpleProducer, KafkaClient
import socket

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 9999                # Reserve a port for your service.
s.bind(("127.0.0.1", port))        # Bind to the port

print("Listening for Drill Sim")
s.listen(1) 
c, addr = s.accept()                # Now wait for client connection.
print("Drill Sim Connected")
message = ""

print("Connecting to Kafka")
kafka = KafkaClient('127.0.0.1:9998')
producer = SimpleProducer(kafka)
print("Kafka Connected")

data = []

while True:
	char = c.recv(1)
	message += char
	if char == "\n":
		split_message = message.strip("\r\n").split(',')
		if len(split_message) == 3 and split_message[0] != '10':
			data.append(split_message[1])
		elif len(split_message) == 3 and split_message[0] == '10':
			data.append(split_message[1])
			data.append(split_message[2])
			final_data = ",".join(data)
			print("Message Sent:%s", final_data)
			producer.send_messages(b'oil-gas', str.encode(final_data))
			data = []

		message = ""



