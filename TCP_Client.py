import socket 
 
host = socket.gethostname() 
port = 10000
BUFFER_SIZE = 2000 
MESSAGE = raw_input("tcpClientA: Enter message/ Enter exit:") 
 
 
#while MESSAGE != 'exit':
#for i in range(0,5):
tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpClientA.connect((host, port))
tcpClientA.send(MESSAGE)     
data = tcpClientA.recv(BUFFER_SIZE)
print "Client received data:",data
 #   MESSAGE = raw_input("tcpClientA: Enter message to continue/ Enter exit:")
tcpClientA.close() 
