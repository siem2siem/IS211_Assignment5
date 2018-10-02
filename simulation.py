#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""IS211 Assignment5 - Sort and Compare.  Tested in Ananconda Spyder Python 2.7.15
The file used for this assignment is:
    http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv
With the simulation.py program and the requests.csv file in the same folder.
The command to run this assignment will take 2 arguments:
    simulation.py --file requests.csv --server 3
with the following output:
    Average Wait time for a request is 2480.00 secs 1664 tasks remaining.
    Average Wait time for a request is 2469.00 secs 1656 tasks remaining.
    Average Wait time for a request is 2489.00 secs 1668 tasks remaining.
"""

import argparse
import csv

parser = argparse.ArgumentParser()
parser.add_argument('--file', type = str, help = 'Name of file to be process.')
parser.add_argument('--servers', type = int, help = 'Number of Servers.')
args = parser.parse_args()

def main():
    """
    Main Function that will take either 1 argument (file) or 2 arguments
    file and number of servers
    """
    if args.file:
        try:
            num_of_servers = args.servers
            if(num_of_servers > 1):
                simulateManyServers(args.file, num_of_servers)
            else:
                simulateOneServer(args.file)
        except:
            print 'There is an error with your input , please try again'   
    else:
        print 'Please use the following format.  --file filename --servers # of Servers'

class Queue:
    """
    Creating a class for queue which have an empty list, 
	 enqueue function to add item to the list
	 dequeue function to remove item from the list
	 size function to provide the current length to the list.
	 """ 
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server:
    """Creating a Server Class""" 
    def __init__(self):        
        self.current_task = None
        self.time_remaining = 0

    def tick(self): 
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self): 
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self,new_task): 
        self.current_task = new_task
        self.time_remaining = new_task.get_length()

class Request:
    """Creating a request class to track user requesting a
    file from webserver.
    """ 
    def __init__(self, time, length):
        self.timestamp = time
        self.length = int(length) 

    def get_stamp(self):
        return self.timestamp 

    def get_length(self):
        return self.length

    def wait_time(self, cur_time): 
        return cur_time - self.timestamp


def simulateOneServer(file):
    """Functions with no server specified where default of 1 is used."""
    server = Server()
    queue = Queue()
    waiting_times = []

    with open(file, 'r') as csvfile:
        response = csv.reader(csvfile, delimiter=',')

        for row in response:
            timestamp = int(row[0])
            requestfile = row[1]
            length =  int(row[2])
            req = Request(timestamp, length)
            queue.enqueue(req)

            if(not server.busy()) and (not queue.is_empty()):
                next_request = queue.dequeue()
                waiting_times.append(next_request.wait_time(timestamp))
                server.start_next(req)
                
            server.tick()

    average_wait = sum(waiting_times) / len(waiting_times)
    print("Average Wait time for a request is %6.2f secs with %3d tasks remaining." % (average_wait, queue.size()))

def simulateManyServers(file, num_of_servers):
    """Functions with more than one server specified """
    servers = []
    for server_count in range(0, num_of_servers):
        servers.append(Server())

    queues = []
    for server_count in range(0, num_of_servers):
        queues.append(Queue()) 

    waiting_times = []
    for server_count in range(0, num_of_servers):
        waiting_times.append([])

    with open(file, 'r') as csvfile:
        response = csv.reader(csvfile, delimiter=',')

        roundRobinPosition = 0  
                                
        for row in response:
            timestamp = int(row[0])
            requestfile = row[1]
            length =  int(row[2])
            req = Request(timestamp, length)

            queues[roundRobinPosition].enqueue(req)
            if roundRobinPosition < num_of_servers - 1:
                roundRobinPosition += 1
            else:
                roundRobinPosition = 0

            if(not servers[roundRobinPosition].busy()) and (not queues[roundRobinPosition].is_empty()):
                next_request = queues[roundRobinPosition].dequeue()
                waiting_times[roundRobinPosition].append(next_request.wait_time(timestamp))
                servers[roundRobinPosition].start_next(req)
                
            servers[roundRobinPosition].tick()

        for server_count in range(0, num_of_servers):
            average_wait = sum(waiting_times[server_count]) / len(waiting_times[server_count])        
            print("Average Wait time for a request is %6.2f secs with %3d tasks remaining." % (average_wait, queues[server_count].size()))

if __name__ == '__main__':
    url = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
    main()
