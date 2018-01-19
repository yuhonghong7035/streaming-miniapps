""" 
Kafka Producer Multiprocess

For light source, broker config (server.properties) needs to be adjusted to allow larger messages:

message.max.bytes = 2086624

"""

from pykafka import KafkaClient
import numpy as np
import os, sys
import time
import datetime
sys.path.append("..")
#import saga_hadoop_utils
import math
from pykafka.partitioners import hashing_partitioner
import uuid
import logging
import pkg_resources
import threading
import base64
import binascii

########################################################################
# Dask
import dask.array as da
import dask.bag as db
from dask.delayed import delayed
from distributed import Client
     
########################################################################
# Default Configuration Variables
KAFKA_HOME="/home/01131/tg804093/work/kafka_2.11-1.0.0"
NUMBER_CLUSTER=100
TOTAL_NUMBER_POINTS=10000
NUMBER_POINTS_PER_CLUSTER=int(math.ceil(float(TOTAL_NUMBER_POINTS)/NUMBER_CLUSTER))
NUMBER_DIM=3 # 1 Point == ~62 Bytes
NUMBER_POINTS_PER_MESSAGE=500 # 3-D Point == 304 KB
INTERVAL=0
NUMBER_OF_PRODUCES=1 # 10*60 = 10 minutes
NUMBER_PARTITIONS=48
TOPIC_NAME="Throughput"
NUMBER_PARALLEL_TASKS=1
NUMBER_NODES=8


########################################################################

def get_random_cluster_points(number_points, number_dim):
    mu = np.random.randn()
    sigma = np.random.randn()
    p = sigma * np.random.randn(number_points, number_dim) + mu
    return p


def produce_block_kmeans_static(block_id=1,
                  kafka_zk_hosts=None,
                  number_clusters_per_partition=NUMBER_CLUSTER,
                  number_points_per_cluster=NUMBER_POINTS_PER_CLUSTER,
                  number_points_per_message = NUMBER_POINTS_PER_MESSAGE,
                  number_dim=NUMBER_DIM,
                  topic_name=TOPIC_NAME):
    start = time.time()
    num_messages = 0
    count_bytes  = 0
    count = 0
    
    print "Zookeeper: %s, Block Id: %s, Num Cluster: %d" % (kafka_zk_hosts, str(block_id), NUMBER_CLUSTER)

    # init Kafka
    client = KafkaClient(zookeeper_hosts=kafka_zk_hosts)
    topic = client.topics[topic_name]
    producer = topic.get_sync_producer(partitioner=hashing_partitioner)
    
    if producer is None: print "Producer None"; return -1
    
    # partition on number clusters
    points = get_random_cluster_points(number_points_per_message, number_dim)
    number_messages = number_points_per_cluster*number_clusters_per_partition/number_points_per_message
    end_data_generation = time.time()
    print "Points Array Shape: %s, Number Batches: %.1f"%(points.shape, number_messages)

    last_index=0
    count_bytes = 0
    for i in range(number_messages):
        logging.debug("Messages#: %d, Points: %d - %d, Points/Message: %d, KBytes: %.1f, KBytes/sec: %s"%\
                                     (num_messages+1,
                                      last_index,                                                                                           
                                      last_index+number_points_per_message, 
                                      number_points_per_message,
                                      count_bytes/1024,
                                      count_bytes/1024/(time.time()-end_data_generation))) 
        points_strlist=str(points.tolist())
        producer.produce(points_strlist, partition_key='{}'.format(count))
        count = count + 1
        last_index = last_index + number_points_per_message
        count_bytes = count_bytes + len(points_strlist)
        num_messages = num_messages + 1
    end = time.time()
    stats = {
        "block_id": block_id,
        "number_messages" :  num_messages,
        "points_per_message": number_points_per_message,
        "bytes_per_message": str(len(points_strlist)),
        "data_generation_time": "%5f"%(end_data_generation-start),
        "transmission_time":  "%.5f"%(end-end_data_generation),
        "runtime": "%.5f"%(end-start)
    }
    return stats 

    
#def produce_block_kmeans(block_id=1,
#                  kafka_zk_hosts=None,
#                  number_clusters_per_partition=NUMBER_CLUSTER,
#                  number_points_per_cluster=NUMBER_POINTS_PER_CLUSTER,
#                  number_points_per_message = NUMBER_POINTS_PER_MESSAGE,
#                  number_dim=NUMBER_DIM,
#                  topic_name=TOPIC_NAME):
#    start = time.time()
#    num_messages = 0
#    count_bytes  = 0
#    count = 0
#    
#    print "Zookeeper: %s, Block Id: %s, Num Cluster: %d" % (kafka_zk_hosts, str(block_id), NUMBER_CLUSTER)
#
#    # init Kafka
#    client = KafkaClient(zookeeper_hosts=kafka_zk_hosts)
#    topic = client.topics[topic_name]
#    producer = topic.get_sync_producer(partitioner=hashing_partitioner)
#    
#    if producer is None: print "Producer None"; return -1
#    
#    # partition on number clusters
#    points = []
#    for i in range(number_clusters_per_partition):    
#        p = get_random_cluster_points(number_points_per_cluster, number_dim)
#        points.append(p)
#    points_np=np.concatenate(points)
#    number_messages = points_np.shape[0]/number_points_per_message
#    end_data_generation = time.time()
#    print "Points Array Shape: %s, Number Batches: %.1f"%(points_np.shape, number_messages)
#
#    last_index=0
#    count_bytes = 0
#    for i in range(number_messages):
#        logging.debug("Messages#: %d, Points: %d - %d, Points/Message: %d, KBytes: %.1f, KBytes/sec: %s"%\
#                                     (num_messages+1,
#                                      last_index,                                                                                           
#                                      last_index+number_points_per_message, 
#                                      
#                                      number_points_per_message,
#                                      count_bytes/1024,
#                                      count_bytes/1024/(time.time()-end_data_generation))) 
#        points_batch = points_np[last_index:last_index+number_points_per_message]
#        points_strlist=str(points_batch.tolist())
#        producer.produce(points_strlist, partition_key='{}'.format(count))
#        count = count + 1
#        last_index = last_index + number_points_per_message
#        count_bytes = count_bytes + len(points_strlist)
#        num_messages = num_messages + 1
#    end = time.time()
#    stats = {
#        "block_id": block_id,
#        "number_messages" :  num_messages,
#        "points_per_message": number_points_per_message,
#        "bytes_per_message": str(len(points_strlist)),
#        "data_generation_time": "%5f"%(end_data_generation-start),
#        "transmission_time":  "%.5f"%(end-end_data_generation),
#        "runtime": "%.5f"%(end-start)
#    }
#    return stats 


def produce_block_kmeans(block_id=1,
                  kafka_zk_hosts=None,
                  number_clusters_per_partition=NUMBER_CLUSTER,
                  number_points_per_cluster=NUMBER_POINTS_PER_CLUSTER,
                  number_points_per_message = NUMBER_POINTS_PER_MESSAGE,
                  number_dim=NUMBER_DIM,
                  topic_name=TOPIC_NAME):
    start = time.time()
    num_messages = 0
    count_bytes  = 0
    count = 0
    
    print "Zookeeper: %s, Block Id: %s, Num Cluster: %d" % (kafka_zk_hosts, str(block_id), NUMBER_CLUSTER)

    # init Kafka
    client = KafkaClient(zookeeper_hosts=kafka_zk_hosts)
    topic = client.topics[topic_name]
    producer = topic.get_sync_producer(partitioner=hashing_partitioner)
    
    if producer is None: print "Producer None"; return -1
    
    # partition on number clusters
    points = []
    for i in range(number_clusters_per_partition):    
        p = get_random_cluster_points(number_points_per_cluster, number_dim)
        points.append(p)
    points_np=np.concatenate(points)
    
    number_messages = points_np.shape[0]/number_points_per_message
    end_data_generation = time.time()
    print "Points Array Shape: %s, Number Batches: %.1f"%(points_np.shape, number_messages)

    last_index=0
    count_bytes = 0
    for i in range(number_messages):
        logging.debug("Messages#: %d, Points: %d - %d, Points/Message: %d, KBytes: %.1f, KBytes/sec: %s"%\
                                     (num_messages+1,
                                      last_index,                                                                                           
                                      last_index+number_points_per_message, 
                                      
                                      number_points_per_message,
                                      count_bytes/1024,
                                      count_bytes/1024/(time.time()-end_data_generation))) 
        points_batch = points_np[last_index:last_index+number_points_per_message]
        points_strlist=str(points_batch.tolist())
        producer.produce(points_strlist, partition_key='{}'.format(count))
        count = count + 1
        last_index = last_index + number_points_per_message
        count_bytes = count_bytes + len(points_strlist)
        num_messages = num_messages + 1
    end = time.time()
    stats = {
        "block_id": block_id,
        "number_messages" :  num_messages,
        "points_per_message": number_points_per_message,
        "bytes_per_message": str(len(points_strlist)),
        "data_generation_time": "%5f"%(end_data_generation-start),
        "transmission_time":  "%.5f"%(end-end_data_generation),
        "runtime": "%.5f"%(end-start)
    }
    return stats 

######################################################################################
# Light Source

def produce_block_light(block_id=1,
                        kafka_zk_hosts=None,
                        number_messages = 1,
                        topic_name=TOPIC_NAME):
    
    start = time.time()
    
    client = KafkaClient(zookeeper_hosts=kafka_zk_hosts)
    topic = client.topics[topic_name]
    producer = topic.get_sync_producer(max_request_size=3086624,
                                       partitioner=hashing_partitioner)
    data = get_lightsource_data()
    #data_b64 = data.encode( 'utf-8' )
    data_enc = binascii.hexlify(data).encode('utf-8')
    data_enc = data_enc
    print "Encoded Type: %s Len: %d"%(str(type(data_enc)),len(data_enc))
    end_data_generation = time.time()
    
    count = 0
    for i in range(number_messages):
        producer.produce(data_enc, partition_key='{}'.format(count))
        count = count+ 1
    end = time.time()
   
    stats = {
        "block_id": block_id, 
        "number_messages" :  number_messages,
        "bytes_per_message_enc": str(len(data_enc)),
        "bytes_per_message_bin": str(len(data)),
        "data_generation_time": "%5f"%(end_data_generation-start),
        "transmission_time":  "%.5f"%(end-end_data_generation),
        "runtime": "%.5f"%(end-start)
    }
    return stats
    
def get_lightsource_data():
    module = "mass"
    data = None
    data_file = pkg_resources.resource_filename(module, "tooth.h5")
    with open(data_file, "r") as f:
        data = f.read()
    print("Access sample data: " + module + "; File: tooth.h5; Size: " + str(len(data)))
    return data

#######################################################################################

class MiniApp():
    
    def __init__(
                 self, 
                 dask_scheduler=None,
                 kafka_zk_hosts=None,
                 number_parallel_tasks=NUMBER_PARALLEL_TASKS,
                 number_clusters=NUMBER_CLUSTER,  # kmeans
                 number_points_per_cluster=NUMBER_POINTS_PER_CLUSTER,  # kmeans
                 number_points_per_message = NUMBER_POINTS_PER_MESSAGE,  # kmeans
                 number_dim=NUMBER_DIM, # kmeans
                 number_messages=1, # light
                 number_produces=NUMBER_OF_PRODUCES,
                 number_partitions=NUMBER_PARTITIONS,
                 topic_name = TOPIC_NAME,
                 application_type = "kmeans", # kmeans or light
                 produce_interval = 0,
                 clean_after_produce = False
                 ):
        
        self.application_type = application_type

        # KMeans specific configuration
        self.number_clusters = number_clusters
        self.number_points_per_cluster = number_points_per_cluster
        self.number_points_per_message = number_points_per_message
        self.number_total_points = self.number_points_per_cluster * self.number_clusters
        self.number_dim=number_dim     
        
        
        if self.application_type == "kmeans" or self.application_type == "kmeans-static":
            self.number_messages = (self.number_points_per_cluster * self.number_clusters)/self.number_points_per_message
        elif self.application_type == "light":
            self.number_messages = number_messages
            self.number_total_points = number_messages # 1 message contains 1 point (image)
            self.number_clusters = -1
            self.number_points_per_cluster = -1
            self.number_points_per_message = -1
            self.number_dim=-1  
            

        
        self.number_parallel_tasks = number_parallel_tasks
        self.number_produces = number_produces
        self.number_partitions = number_partitions
        self.topic_name = topic_name
        self.produce_interval = produce_interval
        self.clean_after_produce = clean_after_produce
        # Kafka / Dask
        self.kafka_zk_hosts = kafka_zk_hosts
        self.kafka_client = KafkaClient(zookeeper_hosts=kafka_zk_hosts)
        self.number_kafka_brokers= len(self.kafka_client.brokers)
        
        self.dask_scheduler = dask_scheduler
        if dask_scheduler is not None:
            self.dask_distributed_client = Client(dask_scheduler)   
        else:
             self.dask_distributed_client = Client(processes=False)  
                
        dask_scheduler_info = self.dask_distributed_client.scheduler_info()
        self.number_dask_workers = len(dask_scheduler_info['workers'])
        self.number_dask_cores_per_worker = dask_scheduler_info['workers'][dask_scheduler_info['workers'].keys()[0]]["ncores"]
          
        print ("Kafka: %s, Dask: %s, Number Dask Nodes: %d,  Number Parallel Producers: %d"%
                                     (self.kafka_zk_hosts, 
                                      str(self.dask_distributed_client.scheduler_info()["address"]),
                                      self.number_dask_workers,
                                      self.number_parallel_tasks
                                     )
              )

    
    def clean_kafka(self):
        cmd="%s/bin/kafka-topics.sh --delete --zookeeper %s --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, self.topic_name)
        print cmd
        #os.system(cmd)
        #time.sleep(60)
    
        cmd="%s/bin/kafka-topics.sh --create --zookeeper %s --replication-factor 1 --partitions %d --topic %s"%\
                                                (KAFKA_HOME, self.kafka_zk_hosts, self.number_partitions, self.topic_name)
        print cmd
        os.system(cmd)
    
        cmd="%s/bin/kafka-topics.sh --describe --zookeeper %s --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, self.topic_name)
        print cmd
        os.system(cmd)
        
 
        
    
    def run(self):          
        run_timestamp=datetime.datetime.now()
        RESULT_FILE= "results/kafka-throughput-producer-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
        try:
            os.makedirs("results")
        except:
            pass
        
        output_file=open(RESULT_FILE, "w")
        output_file.write("Application,Number_Clusters,Number_Points_per_Cluster,Number_Dim,Number_Points_per_Message,Number_Messages,Interval,Number_Partitions,\
Number_Processes,Number_Nodes,Number_Cores_Per_Node, Number_Brokers, Time,Points_per_sec,Records_per_sec,Dask_Config\n")
        
        global_start = time.time()
        count_produces = 0
        self.clean_kafka()
        while count_produces < self.number_produces:
            #if self.clean_after_produce: self.clean_kafka()
            start = time.time()
            # Using Dask Delay API
            tasks = []
            for block_id in range(self.number_parallel_tasks):
                if self.application_type == "kmeans":
                    print "Generate Block ID: " + str(block_id)
                    number_clusters_per_partition = self.number_clusters/self.number_parallel_tasks 
                    #produce_block(block_id, self.kafka_zk_hosts)
                    t = delayed(produce_block_kmeans, pure=False)(
                                                           block_id, 
                                                           self.kafka_zk_hosts,
                                                           number_clusters_per_partition=number_clusters_per_partition,
                                                           number_points_per_cluster=self.number_points_per_cluster,
                                                           number_points_per_message = self.number_points_per_message,
                                                           number_dim=self.number_dim,
                                                           topic_name=self.topic_name
                                                          )
                    tasks.append(t)
                elif self.application_type == "kmeans-static":
                    print "Generate Block ID: " + str(block_id)
                    number_clusters_per_partition = self.number_clusters/self.number_parallel_tasks 
                    #produce_block_kmeans_static(block_id, self.kafka_zk_hosts)
                    t = delayed(produce_block_kmeans_static, pure=False)(
                                                           block_id, 
                                                           self.kafka_zk_hosts,
                                                           number_clusters_per_partition=number_clusters_per_partition,
                                                           number_points_per_cluster=self.number_points_per_cluster,
                                                           number_points_per_message = self.number_points_per_message,
                                                           number_dim=self.number_dim,
                                                           topic_name=self.topic_name
                                                          )
                    tasks.append(t)
                    
                elif self.application_type == "light":
                    number_messages_per_task = self.number_messages/self.number_parallel_tasks
                    t = delayed(produce_block_light, pure=False)(
                                                           block_id, 
                                                           self.kafka_zk_hosts,
                                                           number_messages_per_task,
                                                           topic_name=self.topic_name
                                                          )
                    tasks.append(t)
                else:
                    print "Unknown Application/Data Source Type"
                
                
            print "Waiting for Dask Tasks to complete"
            res = delayed(tasks).compute()
            print str(res)
            print "End Produce via Dask"
            end = time.time()
             
            print "Number: %d, Number Parallel Tasks: %d, Runtime: %.1f"%(count_produces, 
                                                                          self.number_parallel_tasks, 
                                                                                       end-start)
            output_file.write(
                               "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.5f,%.5f,%.5f,dask-distributed\n"%\
                               (
                                  self.application_type,
                                  self.number_clusters,
                                  self.number_points_per_cluster,
                                  self.number_dim, 
                                  self.number_points_per_message,
                                  self.number_messages,
                                  INTERVAL,
                                  NUMBER_PARTITIONS,
                                  self.number_parallel_tasks, 
                                  self.number_dask_workers,
                                  self.number_dask_cores_per_worker,
                                  self.number_kafka_brokers,
                                  (end-start), 
                                  (self.number_total_points/(end-start)), 
                                  (self.number_messages/(end-start))
                                )
                               )
            output_file.flush()
            count_produces = count_produces + 1
        
            time.sleep(self.produce_interval)
        
        output_file.close()
        
        
        
    def run_in_background(self):
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True   # Daemonize thread
        self.thread.start()   
    
    def wait(self):
        self.thread.join()
        
    def cancel(self):
        self.thread.cancel()
        