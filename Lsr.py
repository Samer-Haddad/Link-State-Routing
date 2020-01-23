# Written by Samer Alhaddad for COMP9331 19T2 Assignment "Link-State Routing"
# Python 3.7.4
import sys
import os
import threading
import time
import socket
import pickle
import queue
from collections import defaultdict, namedtuple

class LinkStatePacket:
    def __init__(self):                                  
        self.type = 1
        self.src = ID
        #self.port = PORT
        self.seq = 0
        self.visited = list()
        self.content = defaultdict(float)

class HeartbeatPacket:
    def __init__(self):
        self.type = 2
        self.src = ID

class NodeFailurePacket:
    def __init__(self, src):
        self.type = 3
        self.src = src
        self.visited = [ID]
        self.failTime = time.time()

class Neighbour(threading.Thread):
    def __init__(self):
        super(Neighbour, self).__init__()
        self.id = str                                               # Node ID
        self.port = int                                             # Node port
        self.cost = float                                           # Link cost
        self.state = bool                                           # Node last known state
        self.Q = queue.Queue(maxsize=5)                             # Queue to store incoming heartbeat packets. used by threads Neighbour and listen

    def kill(self):                                                 # Function to determine failed neighbour node
        if self.state == 1:
            if ((Nodes[self.id].state == 1) or not(self.id in fNodes)):
                #print('Node', self.id, 'Failed')
                global failPoint
                failPoint = time.time()
                NFP = NodeFailurePacket(self.id)
                reTransmitQueue.put(NFP)
                reTransmitQueue.put(NFP)
                fNodes.append(self.id)
                Nodes[self.id].kill()
                self.state = 0
            else:                                             
                self.state = 0

    def keepAlive(self):
        try:
            self.Q.put_nowait(1)
        except queue.Full:
            pass                                                   
        
    def activate(self):
        try:
            self.Q.put_nowait(1)
        except queue.Full:
            pass                   
        self.state = 1

    def run(self):
        while True:
            time.sleep(0.6)
            try:
                self.Q.get_nowait()
                self.Q.queue.clear()
            except queue.Empty:
                self.kill()

class Node:
    def __init__(self):
        self.seq = int                                              # Node Last recorded packet sequence
        self.state = bool                                           # Node last known state
    def activate(self):
        self.state = 1
    def kill(self):
        self.state = 0
        self.seq = 0

class Listen(threading.Thread):
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((HOST, PORT))
        global failPoint

        while True:
            packet, _ = sock.recvfrom(1024)                         # address isn't important.
            packet = pickle.loads(packet)
            src = packet.src

            if packet.type == 2:                                    # Heartbeat Packet
                Neighbours[src].keepAlive()
                if src in fNodes:
                    fNodes.remove(src)
                    Neighbours[src].activate()
                    Nodes[src].activate()
                    #print('Node', packet.src, 'Re-Activated')
                    continue
                elif Neighbours[src].is_alive() is False:
                    Neighbours[src].start()
                    Neighbours[src].activate()
                    Nodes[src].activate()


            elif packet.type == 1:                                  # Link-State Packet
                #print(packet.src, ' #',packet.seq, ' Content:', packet.content)
                if src in Nodes:
                    node = Nodes[src]
                    if ((packet.seq) > (node.seq)):
                        node.seq = packet.seq
                        reTransmitQueue.put(packet)
                        TOPOLOGY[src] = packet.content
                        if src in fNodes:
                            fNodes.remove(src)
                            node.activate()
                        continue
                    else:
                        continue
                else:
                    node = Nodes[src]
                    node.seq = packet.seq
                    node.activate()
                    reTransmitQueue.put(packet)
                    TOPOLOGY[src] = packet.content
                    continue

            elif packet.type == 3:                                  # Node-Failure Packet
                node = Nodes[src]
                if not(src in fNodes):
                    #print('Node', packet.src, 'Failed')
                    reTransmitQueue.put(packet)
                    fNodes.append(src)
                    node.kill()
                    failPoint = packet.failTime
                else:
                    reTransmitQueue.put(packet)

class ReTransmit(threading.Thread):
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        reTransmitTo = []
        while True:
            try:
                packet = reTransmitQueue.get()
            except queue.Empty:                                     # This code won't be reached since queue.get() blocks until
                continue                                            # element received, but just being extra cautious.
                
            for node in Neighbours:
                if (not(node in packet.visited) and (Neighbours[node].state == 1)):
                    packet.visited.append(node)
                    reTransmitTo.append(Neighbours[node].port)

            packet = pickle.dumps(packet)

            if reTransmitTo:
                for neighbour_port in reTransmitTo:
                    sock.sendto(packet, (HOST,neighbour_port))
                    
            reTransmitTo.clear()

class Broadcast(threading.Thread):
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        LSA = LinkStatePacket()
        while True:
            LSA.visited = [ID]
            broadcastTo = []

            for node in Neighbours:
                if Neighbours[node].state == 1:
                    nport = Neighbours[node].port
                    ncost = Neighbours[node].cost
                    LSA.content[node] = ncost
                    LSA.visited.append(node)
                    broadcastTo.append(nport)

            if broadcastTo:
                LSA.seq += 1
                packet = pickle.dumps(LSA)
                for neighbour_port in broadcastTo:
                    sock.sendto(packet, (HOST,neighbour_port))

            LSA.content.clear()
            time.sleep(1)

class Heartbeat(threading.Thread):
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        HB = HeartbeatPacket()
        while True:
            packet = pickle.dumps(HB)
            for node in Neighbours:
                if not(node in fNodes):
                    neighbour_port = Neighbours[node].port
                    sock.sendto(packet, (HOST,neighbour_port))
            time.sleep(0.2)

class Dijkstra(threading.Thread):
    def run(self):

        global failPoint
        Edge = namedtuple('Edge', 'Cost Pred')

        def path(node):
            if Edges[node].Cost == 0.0:
                return [node]
            else:
                return [node] + path(Edges[node].Pred)

        while True:
            time.sleep(30)

            while failPoint:
                delay = 60 - (time.time() - failPoint)
                #print('Delay: ', f'{delay:.1f}', 'sec')
                failPoint = 0.0
                time.sleep(delay)
                
            Edges = defaultdict(Edge)
            Top = TOPOLOGY

            if fNodes:
                for node in fNodes:
                    if node in Top:
                        del Top[node]
            
            Edges[ID] = Edge(0.0, ID)
            initCost = float("inf")
            initNode = ''

            for node in Top:
                if node in Neighbours:
                    cost = Neighbours[node].cost
                    Edges[node] = Edge(cost, ID)
                    if cost < initCost:
                        initCost = cost
                        initNode = node

            for neighbour in Top[initNode]:
                if not(neighbour == ID):
                    ncost = Top[initNode][neighbour]
                    pred_cost = Edges[initNode].Cost
                    new_cost = pred_cost + ncost
                    if not(neighbour in Edges):
                        Edges[neighbour] = Edge(new_cost, initNode)
                    else:
                        current_cost = Edges[neighbour].Cost
                        if new_cost < current_cost:
                            Edges[neighbour] = Edge(new_cost, initNode)

            del Top[initNode]

            for _ in range(len(TOPOLOGY)):
                for node in Edges:
                    if ((node in Top) and not(node == ID)):
                        for neighbour in Top[node]:
                            if not(neighbour == ID):
                                ncost = Top[node][neighbour]
                                pred_cost = Edges[node].Cost
                                new_cost = pred_cost + ncost
                                if not(neighbour in Edges):
                                    Edges[neighbour] = Edge(new_cost, node)
                                else:
                                    current_cost = Edges[neighbour].Cost
                                    if new_cost < current_cost:
                                        Edges[neighbour] = Edge(new_cost, node)
                        del Top[node]
                        break         
            
            print('I am Router', ID)
            for node in Edges:
                if not(node == ID):
                    p = path(node)
                    p = reversed(p)
                    print('Leasat cost path to router', f'{node}:'+''.join(str(x) for x in p), 'and the cost is', f'{Edges[node].Cost:.1f}')


Nodes = defaultdict(Node)
Neighbours = defaultdict(Neighbour)
HOST = '127.0.0.1'
PORT = int
ID = str
number_of_neighbours = int
TOPOLOGY = defaultdict(dict)
reTransmitQueue = queue.Queue(maxsize=0)
fNodes = list()
failPoint = 0.0

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('Input Error. Usage: python Lsr.py [config.txt].')
        sys.exit()

    config = sys.argv[1]

    if not os.path.exists(config):
        print(f'Input Error. There is no file named "{config}" in the working directory')
        sys.exit()

    with open(config, newline='') as file:
        for line in file:
            line = line.split()
            ID, PORT = line[0], int(line[1])
            break
        for line in file:
            line = line.split()
            number_of_neighbours = line[0]
            break
        for line in file:
            line = line.split()
            node, ncost, nport = line[0], float(line[1]), int(line[2])
            Neighbours[node].id = node
            Neighbours[node].port = nport
            Neighbours[node].cost = ncost
            Neighbours[node].state = 0
            Nodes[node].state = 0
            Nodes[node].seq = 0
    file.close()

    heartbeat = Heartbeat(name='HEARTBEAT')
    listen = Listen(name='LISTEN')
    broadcast = Broadcast(name='BROADCAST')
    retransmit = ReTransmit(name='RETRANSMIT')
    dijkstra = Dijkstra(name='DIJKSTRA')

    heartbeat.start()
    listen.start()
    broadcast.start()
    retransmit.start()
    dijkstra.start()
    #for node in Neighbours:
        #Neighbours[node].start()
    
    