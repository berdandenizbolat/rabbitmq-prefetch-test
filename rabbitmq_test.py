import pika
from matplotlib import pyplot as plt
import numpy as np
import threading
import time
import os
from dotenv import load_dotenv
import json
from pyrabbit.api import Client
import math
import pickle

class senderThread(threading.Thread):
        def __init__(self, name):
                threading.Thread.__init__(self)
                self.name = name
        
        def run(self):
            print("Thread " + str(self.name) + " starts sending messages...")
            sendMessages()

class receiverThread(threading.Thread):
        def __init__(self, name):
                threading.Thread.__init__(self)
                self.name = name
        
        def run(self):
            print("Thread " + str(self.name) + " starts receiving messages...")
            receiveMessages()


class listenerThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            receiveQueueMessageCount()

class storeClass:
        def __init__(self, prefetchId, rate, timeList, messageList, ackList):
            self.prefetchId = prefetchId
            self.rate = rate
            self.timeList = timeList
            self.messageList = messageList
            self.ackList = ackList

def saveStoreClass(storeElem):
        with open("./" + str(storeElem.prefetchId) + "_" + str(storeElem.rate) + ".pickle", 'wb') as file:
            pickle.dump(storeElem, file)

def loadStoreClass(prefetchId, rate):
        with open("./" + prefetchId + "_" + rate + ".pickle", 'rb') as file:
            return pickle.load(file)


def sendMessages():
    sendCount = 0
    while (len(TIME_COUNT) < 1000):
        time.sleep(SEND_RATE)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.environ.get("RABBITMQ_URL"))
        )
        channel = connection.channel()

        channel.queue_declare(queue='queue_example', durable=True)
        message = json.loads(os.environ.get("MESSAGE_EXAMPLE"))

        channel.basic_publish(
            exchange='',
            routing_key='queue_example',
            body=str(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
        sendCount += 1
        connection.close()

def receiveMessages():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host = os.environ.get("RABBITMQ_URL"))
    )

    channel = connection.channel()
    channel.queue_declare(queue = 'queue_example', durable = True)

    channel.basic_qos(prefetch_count = PREFETCH_COUNT)
    channel.basic_consume(queue = 'queue_example', on_message_callback=callback)

    channel.start_consuming()

def deleteQueue():
    print("Deleting all messages in the queue started")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host = os.environ.get("RABBITMQ_URL"))
    )

    channel = connection.channel()
    channel.queue_delete(queue = "queue_example")
    print("Deleting all messages in the queue finished")
    return


def insertThousandMessages():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("RABBITMQ_URL"))
    )
    channel = connection.channel()
    channel.queue_declare(queue='queue_example', durable=True)
    message = json.loads(os.environ.get("MESSAGE_EXAMPLE"))
    for i in range(0, 1000):
        channel.basic_publish(
            exchange='',
            routing_key='queue_example',
            body=str(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
    print("Thousand Message is inserted")
    connection.close()

def callback(ch, method, properties, body):
    if (len(LISTENER_STATUS) >= 1):
        ch.close()
        return
    while True:
        if (RECEIVE_BOOL):
            time.sleep(RECEIVE_RATE)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            break
        if(len(LISTENER_STATUS) >= 1):
            ch.close()
            return

def receiveQueueMessageCount():
    count = 0
    cl = Client(os.environ.get("CLIENT_ADRESS"), 'guest', 'guest')
    vhosts = [i['name'] for i in cl.get_all_vhosts()]
    queues = [q['name'] for q in cl.get_queues()]
    try:
        while count < 1000:
            time.sleep(1)
            TIME_COUNT.append(count)
            queueSearched = cl.get_queue(vhosts[0], queues[0])
            queueSearchedLength = cl.get_queue_depth(vhosts[0], queues[0])
            MESSAGE_COUNT.append(queueSearched["messages_ready"])
            try:
                print(queueSearchedLength)
                ACK_COUNT.append(queueSearchedLength)
            except:
                ACK_COUNT.append(0)
            print(MESSAGE_COUNT[-1])
            print(ACK_COUNT[-1])
            count += 1
        LISTENER_STATUS.append("end")

    except:
        return


load_dotenv()

SEND_INTERVAL = (2, 10)
# SEND_RATE = (np.random.randint(SEND_INTERVAL[0], SEND_INTERVAL[1]))/100
RECEIVE_INTERVAL = (5, 15)
# RECEIVE_RATE = (np.random.randint(RECEIVE_INTERVAL[0], RECEIVE_INTERVAL[1]))/100

SEND_RATE = 0.2
RECEIVE_RATE = 0.015

THREAD_COUNT = 20
PREFETCH_COUNT = 1
TIME_COUNT = []
MESSAGE_COUNT = []
ACK_COUNT = []
LISTENER_STATUS = []
RECEIVE_BOOL = True



#prefetch_list = [1, 5, 10, 50]
prefetch_list = [1, 5]
#receive_rate_factor_list = [1.1, 1.2, 1.3, 1.45, 1.5, 1.55, 1.6, 1.65]
receive_rate_factor_list = [1.1]

for receive_rate in receive_rate_factor_list:
    for prefetch in prefetch_list:
        deleteQueue()
        RECEIVE_BOOL = False
        time.sleep(10)
        RECEIVE_RATE = receive_rate * SEND_RATE
        MESSAGE_COUNT = []
        TIME_COUNT = []
        ACK_COUNT = []
        LISTENER_STATUS = []
        senders = []
        receivers = []
        sender_count = 0
        PREFETCH_COUNT = prefetch
        insertThousandMessages()
        time.sleep(15)
        listener = listenerThread()
        listener.start()
        for i in range(1, THREAD_COUNT + 1):
            thread_receiver = receiverThread(i)
            thread_receiver.start()
            receivers.append(thread_receiver)
        time.sleep(10)
        for i in range(1, THREAD_COUNT + 1):
            thread_sender = senderThread(i)
            senders.append(thread_sender)
            thread_sender.start()
        RECEIVE_BOOL = True
        for sender in senders:
            sender.join()
        listener.join()
        print("Listener joined")
        for receiver in receivers:
            print("Receiver joined")
            receiver.join()
        plt.plot(TIME_COUNT, MESSAGE_COUNT)
        plt.ylabel("Message Count in the Queue")
        plt.xlabel("Time")
        title = "prefetch = " + str(prefetch) + ", consuming delay is " + str(math.trunc(receive_rate*100)) + "% of the publishing delay"
        plt.title(title)
        plt.savefig(title + ".png")
        plt.figure().clear()
        plt.plot(TIME_COUNT, ACK_COUNT)
        plt.ylabel("Queue + Prefetch Buffers Total Message Count")
        plt.xlabel("Time")
        title = "prefetch = " + str(prefetch) + ", consuming delay is " + str(math.trunc(receive_rate*100)) + "% of the publishing delay"
        plt.title(title)
        plt.savefig("ack_count" + title + ".png")
        plt.figure().clear()
        registerElem = storeClass(prefetch, receive_rate, TIME_COUNT, MESSAGE_COUNT, ACK_COUNT)
        saveStoreClass(registerElem)

print("TEST FINISHED...")















