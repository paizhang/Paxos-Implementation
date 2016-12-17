from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import socket
import sys
import threading
import logging
import time

class accepterRPCServer(SimpleXMLRPCServer):
    def serve_forever(self):
        self.quit = False
        while not self.quit:
            self.handle_request()

class acceptor(threading.Thread):   #Accepter thread
    def __init__(self,server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip
        self.id = 0  #Initialize the accepter id using lamport clock
        #self._stop = threading.Event()
        self.server = accepterRPCServer((self.ip, 10030), logRequests=True)

    def receive_proposal(self,id):   #Receive the proposal in the first frame
        logging.info("acceptor id is" + str(self.id))
        if id > self.id:   #If the id received is greater than the id of previous promised proposal, send "Ppomise" message
            return "Promise"
        else:   #If the id received is not greater than the id of previous promised proposal, send "Refuse" message containing the previous id
            return "Refuse " + str(self.id)   

    def accept_request(self,id):   #In the second phase, the accepter accepts the proposal
        if self.id < id:
            self.id = id
            return "Accepted"
        else:
            return "Ignored"    
    
    def stop(self):
        logging.info("Stop Accepter thread.")
        self.server.quit = True
    
    def run(self):
        #server = accepterRPCServer((self.ip, 10030), logRequests=True)
        self.server.register_function(self.receive_proposal)
        self.server.register_function(self.accept_request)
        logging.info("Start acceptor server")
        self.server.serve_forever()

class learner(threading.Thread):    #Learner thread
    def __init__(self,server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip
        
    def execute_request(self,request):   #The learner will execute the request
        #print "This is learner server..."
        command = request.split(',')
        #print request
        action = command[0]
        #logging.info("Received the request: %s")%(request)
        if len(command) <= 1 or len(command) >= 4:  #Validate the request
            reply = "Request is illegal!"
        elif action != 'PUT' and action != 'GET' and action != 'DELETE':
            reply = "Request is malformed!"
        elif action == 'PUT':  #Process the PUT request
            if len(command) == 3:
                if mutex.acquire(1):
                    if key_value_dic.has_key(command[1]) == False:
                        key_value_dic[command[1]] = command[2]
                        reply = "PUT success:(" + command[1] + "," + command[2] + ")"
                    else:
                        reply = "PUT fail: the key " + command[1] + "has already existed!"
                    mutex.release()
            else:
                reply = "PUT request is malformed!"
        elif action == 'GET':  #Process the GET request
            if mutex.acquire(1):
                if key_value_dic.has_key(command[1]) == True:
                    reply = "GET success: value is " + key_value_dic[command[1]]
                else:
                    reply = "GET fail: the key does not exist!"
                mutex.release()
        elif action == 'DELETE':  #Process the DELETE request
            if mutex.acquire(1):
                if key_value_dic.has_key(command[1]) == True:
                    del key_value_dic[command[1]]
                    reply = "DELETE success!"
                else:
                    reply = "DELETE fail: the key does not exist!"
                mutex.release()
        logging.info("The response for the request from is:%s"%(reply))
        logging.info("Response has sent!")
        return reply
    
    def run(self):
        server = SimpleXMLRPCServer((self.ip, 10050), logRequests=True)
        server.register_function(self.execute_request)
        logging.info("Start learner server")
        server.serve_forever()

num_of_success = 0
class proposer:
    def __init__(self):    #initialize the proposer id using lamport clock
        self.id = 0        

    def send_proposal(self,request):  
        #print "This is the proposer!"
        print request
        res = ""
        file = open('server_list.txt')
        server_list = []
        while 1:   #Get the server list in the file
            line = file.readline()
            if not line:
                break
            line = line.strip('\n')
            server_list.append(line)
        #print server_list
        num_server = len(server_list)
        num_acceptor_promise = 0
        #socket.setdefaulttimeout(5)
        while True:
            #print "This is while loop"
            socket.setdefaulttimeout(5)    #Set the timeout for the RPC when calling the acceptor and the learner
            for server_info in server_list: 
                try:
                    rpc_server_path = "http://" + server_info.split(' ')[0] + ":10030" 
                    #print rpc_server_path
                    proxy = xmlrpclib.ServerProxy(rpc_server_path)
                    logging.info("Start calling accepter server...")
                    self.id += 1
                    logging.info("proposer id is" + str(self.id))
                    reply = proxy.receive_proposal(self.id)    #In the first phase, the proposer sends id to the acceptor and waits for the reply
                    if reply != "Promise":    #If the acceptor refuse the id of the proposer, proposer sets the value of the id to a higher value
                        higher_id = int(reply.split(' ')[1])
                        logging.info("higher id is" + str(higher_id))
                        self.id = higher_id + 1
                    else:   
                        num_acceptor_promise += 1
                except socket.error, detail:   #If one accepter fails, just continue to connect with another accepter
                    logging.info("Connection refused from the acceptor:" + server_info.split(' ')[0])
                    continue
            if num_acceptor_promise >= (num_server/2+1):    #If the majority of accepter promise to the proposer, go to the second phase
                for server_info in server_list:
                    try:
                        rpc_server_path = "http://" + server_info.split(' ')[0] + ":10030"
                        proxy = xmlrpclib.ServerProxy(rpc_server_path)
                        reply = proxy.accept_request(self.id)   #Send accept message to the majority of accepters
                        logging.info("Responce from acceptor:" + server_info.split(' ')[0] + " is " + reply)
                    except socket.error, detail:  #Ignore the accepter which fails
                        logging.info("Connection refused from the acceptor:" + server_info.split(' ')[0])
                        continue
                res_dic = {}
                for server_info in server_list:
                    try:
                        rpc_server_path = "http://" + server_info.split(' ')[0] + ":10050"
                        proxy = xmlrpclib.ServerProxy(rpc_server_path)
                        logging.info("Start calling learner server")
                        reply = proxy.execute_request(request)  #Inform the learner to execute the request
                        if res_dic.has_key(reply):   
                            res_dic[reply] += 1
                        else:
                            res_dic[reply] = 0
                        logging.info("Responce from learner:" + server_info.split(' ')[0] + " is " + reply)
                    except socket.error, detail:
                        logging.info("Connection refused from the acceptor:" + server_info.split(' ')[0])
                        continue
                res = max(res_dic, key=lambda i: res_dic[i])   #Return the responce from the quorum
                #print res
                break
            else:
                continue
        num_of_success += 1
        #print "number of success thread is " + str(num_of_success)
        return res

ps = proposer()

class client_request_thread(threading.Thread):   #Thread to process client request
    #Initiate the thread
    def __init__(self,client_ip,client_port,connection):
        threading.Thread.__init__(self)
        self.ip = client_ip
        self.port = client_port
        self.conn = connection
        logging.info("Start new thread from %s:%d"%(self.ip,self.port))

    def run(self): #Process the request from client
        logging.info("Process connection from %s:%d"%(self.ip,self.port)) 
        transaction = ''
        try:
            data = self.conn.recv(100)
            command = data.split(',')
            action = command[0]
            logging.info("Received the request: %s from %s:%d"%(data,self.ip,self.port))
            #ps = proposer()
            reply = ps.send_proposal(data)  #Start proposer
            logging.info("Response to %s:%d has sent!"%(self.ip,self.port))
            self.conn.sendall(reply)  #Send back the responce to the client
        except:
            logging.debug("connection broken")
        finally:
            self.conn.close()
            logging.info("Connection with %s:%d closed"%(self.ip,self.port))

class listen_client_request(threading.Thread):   #Listening request from the client
    def __init__(self,server_ip,server_port):
        threading.Thread.__init__(self)
        self.ip = server_ip
        self.port = server_port
        
    def run(self):
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        serverAddress = (self.ip,self.port)
        logging.info("server name is %s and server port is %s"%serverAddress)
        sock.bind(serverAddress)
        logging.info("binding succeed!")

        #sock.listen(1)
        #ps = proposer()
        #ps.send_proposal()
        while 1:
            sock.listen(1)
            logging.info("waiting for a client request...")
            connection, (client_ip,client_port) = sock.accept()
            new_thread = client_request_thread(client_ip,client_port,connection)  #Create new thread to for the connection
            new_thread.start()  #Start thread


    
def start_server(server_name, server_port):   #Start the server
    listen_client_thread = listen_client_request(server_name,server_port)
    listen_client_thread.start()       
    acceptor_server_thread = acceptor(server_name)
    acceptor_server_thread.start()
    learner_server_thread = learner(server_name)    
    learner_server_thread.start()
    #time.sleep(5) 
    #acceptor_server_thread.exit()
    #time.sleep(5)
    #accepter_server_thread_1 = acceptor(server_name)
    #accepter_server_thread_1.start()
    
key_value_dic= {}
mutex = threading.Lock()

"""
Set log configuration. 
"""
def log_setting():
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s.%(msecs)03d %(filename)s %(levelname)s: %(message)s',datefmt="%Y-%m-%d %H:%M:%S",filename='TCP_Server.log',filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

if __name__ == "__main__":
    server_name = socket.gethostbyname(socket.gethostname())  #Get local ip address
    argc = len(sys.argv)  #Get the total number of command line argument
    log_setting()  #Setting log configuration
    if argc == 2:  #Vaild number of argument
        server_port = int(sys.argv[1])
        if server_port > 0 and server_port <= 65535:  #Check the input ip port
            start_server(server_name, server_port)  #Start tcp server
        else:
            print "Invalid IP Port!"
            exit(1)
    else:
        print "Usage: %s port" % sys.argv[0]
        exit(1)    
