### Kafka Producer Multiprocess

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
########################################################################
# Dask
import dask.array as da
import dask.bag as db
from dask.delayed import delayed
from distributed import Client
     
########################################################################



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

def test(block_id):
    print "hello: " + str(block_id)
    return block_id


#############################################################

def get_random_cluster_points(number_points, number_dim):
    mu = np.random.randn()
    sigma = np.random.randn()
    p = sigma * np.random.randn(number_points, number_dim) + mu
    return p
    
def produce_block(block_id=1,
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
  

class MiniApp():
    
    def __init__(
                 self, 
                 dask_scheduler=None,
                 kafka_zk_hosts=None,
                 number_parallel_tasks=NUMBER_PARALLEL_TASKS,
                 number_clusters=NUMBER_CLUSTER,
                 number_points_per_cluster=NUMBER_POINTS_PER_CLUSTER,
                 number_points_per_message = NUMBER_POINTS_PER_MESSAGE,
                 number_dim=NUMBER_DIM,
                 number_produces=NUMBER_OF_PRODUCES,
                 number_partitions=NUMBER_PARTITIONS,
                 topic_name = TOPIC_NAME
                 ):
        
        self.number_parallel_tasks = number_parallel_tasks
        self.kafka_zk_hosts = kafka_zk_hosts
        self.dask_scheduler = dask_scheduler
        self.number_clusters = number_clusters
        self.number_points_per_cluster = number_points_per_cluster
        self.number_points_per_message = number_points_per_message
        self.number_dim=number_dim      
        self.number_produces=number_produces
        self.number_partitions=number_partitions
        self.topic_name = topic_name
        
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
        cmd="%s/bin/kafka-topics.sh --delete --zookeeper %s --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, TOPIC_NAME)
        print cmd
        #os.system(cmd)
        #time.sleep(60)
    
        cmd="%s/bin/kafka-topics.sh --create --zookeeper %s --replication-factor 1 --partitions %d --topic %s"%\
                                                (KAFKA_HOME, self.kafka_zk_hosts, self.number_partitions, TOPIC_NAME)
        print cmd
        os.system(cmd)
    
        cmd="%s/bin/kafka-topics.sh --describe --zookeeper %s --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, TOPIC_NAME)
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
        output_file.write("Number_Clusters,Number_Points_per_Cluster,Number_Dim,Number_Points_per_Message,Interval,Number_Partitions,\
Number_Processes,Number_Nodes,Number_Cores_Per_Node, Time,Points_per_sec,Records_per_sec,Dask_Config\n")
        
        global_start = time.time()
        count_produces = 0
        while count_produces < self.number_produces:
            self.clean_kafka()
            start = time.time()
            # Using Dask Delay API
            tasks = []
            for block_id in range(self.number_parallel_tasks):
                number_clusters_per_partition = self.number_clusters/self.number_parallel_tasks 
                print "Generate Block ID: " + str(block_id)
                #produce_block(block_id, self.kafka_zk_hosts)
                t = delayed(produce_block, pure=False)(
                                                       block_id, 
                                                       self.kafka_zk_hosts,
                                                       number_clusters_per_partition=number_clusters_per_partition,
                                                       number_points_per_cluster=self.number_points_per_cluster,
                                                       number_points_per_message = self.number_points_per_message,
                                                       number_dim=self.number_dim,
                                                       topic_name=self.topic_name
                                                      )
                tasks.append(t)
                
            print "Waiting for Dask Tasks to complete"
            res = delayed(tasks).compute()
            print str(res)
            print "End Produce via Dask"
            end = time.time()
            
             
            print "Number: %d, Number Parallel Tasks: %d, Time to produce %d points: %.1f"%(count_produces, self.number_parallel_tasks, 
                                                                                       self.number_clusters*self.number_points_per_cluster, 
                                                                                       end-start)
            output_file.write(
                               "%d,%d,%d,%d,%d,%d,%d,%d,%d,%.5f,%.5f,%.5f,dask-distributed\n"%\
                               (
                                  self.number_clusters,
                                  self.number_points_per_cluster,
                                  self.number_dim, 
                                  self.number_points_per_message,
                                  INTERVAL,
                                  NUMBER_PARTITIONS,
                                  self.number_parallel_tasks, 
                                  self.number_dask_workers,
                                  self.number_dask_cores_per_worker,
                                  (end-start), 
                                  ((self.number_clusters*self.number_points_per_cluster)/(end-start)),
                                  (((self.number_clusters*self.number_points_per_cluster)/self.number_points_per_message)/(end-start))
                                )
                               )
            output_file.flush()
            count_produces = count_produces + 1
        
            time.sleep(INTERVAL)
        
        output_file.close()
        