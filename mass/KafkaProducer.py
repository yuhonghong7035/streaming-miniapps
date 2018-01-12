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

########################################################################
# Dask
import dask.array as da
import dask.bag as db
from dask.delayed import delayed
from distributed import Client
     
########################################################################



KAFKA_HOME="/home/01131/tg804093/work/kafka_2.11-1.0.0"
NUMBER_CLUSTER=192
TOTAL_NUMBER_POINTS=10000
NUMBER_POINTS_PER_CLUSTER=int(math.ceil(float(TOTAL_NUMBER_POINTS)/NUMBER_CLUSTER))


NUMBER_DIM=3 # 1 Point == ~62 Bytes
NUMBER_POINTS_PER_MESSAGE=500 # 3-D Point == 304 KB
#NUMBER_POINTS_PER_MESSAGE=[10000] # 3-D Point == 304 KB
INTERVAL=0
NUMBER_OF_PRODUCES=1 # 10*60 = 10 minutes
NUMBER_PARTITIONS=48
TOPIC_NAME="Throughput"
NUMBER_PARALLEL_TASKS=1
NUMBER_NODES=8
print "**************************************\nUse %d Parallel Tasks:"%NUMBER_PARALLEL_TASKS

#zkKafka=saga_hadoop_utils.get_kafka_config_details(os.path.expanduser('~'))[1]

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
                  number_clusters=NUMBER_CLUSTER,
                  num_points_per_message = NUMBER_POINTS_PER_MESSAGE,
                  number_dim=NUMBER_DIM):
    #global num_messages
    #global bytes  
    #global count
    
    num_messages = 0
    count_bytes  = 0
    count = 0
    
    print "Zookeeper: %s, Block Id: %s, Num Cluster: %d" % (kafka_zk_hosts, str(block_id), NUMBER_CLUSTER)
    #print "Key-Value Args: " + str(kwargs)

    # init Kafka
    client = KafkaClient(zookeeper_hosts=kafka_zk_hosts)
    topic = client.topics[TOPIC_NAME]
    producer = topic.get_sync_producer(partitioner=hashing_partitioner)
    
    if producer is None:
        print "Producer None" 
    
    # partition on number clusters
    num_cluster_partition = NUMBER_CLUSTER/NUMBER_PARALLEL_TASKS 
    
    points = []
    for i in range(num_cluster_partition):    
        p = get_random_cluster_points(NUMBER_POINTS_PER_CLUSTER, NUMBER_DIM)
        points.append(p)
    points_np=np.concatenate(points)
    #print points_np.shape
    number_batches = points_np.shape[0]/NUMBER_POINTS_PER_MESSAGE
    print "Points Array Shape: %s, Number Batches: %.1f"%(points_np.shape, number_batches)
    last_index=0
    for i in range(number_batches):
        #print "Produce Batch: %d - %d, Num Messages: %d, Number Points per Message: %d, KBytes Transfered: %.1f, KBytes/sec: %s"%\
        #                             (last_index,                                                                                           
        #                              last_index+num_points_per_message, 
        #                              num_messages,
        #                              num_points_per_message,
        #                              count_bytes/1024,
        #                              count_bytes/1024/(time.time()-global_start))
        print "Produce Batch: %d - %d, Num Messages: %d, Number Points per Message: %d"%\
                                        (last_index,                                                                                           
                                         last_index+num_points_per_message, 
                                         num_messages,
                                         num_points_per_message)
        
        points_batch = points_np[last_index:last_index+num_points_per_message]
        points_strlist=str(points_batch.tolist())
        producer.produce(points_strlist, partition_key='{}'.format(count))
        count = count + 1
        last_index = last_index + num_points_per_message
        count_bytes = count_bytes + len(points_strlist)
        num_messages = num_messages + 1
    return count 
  

class MiniApp():
    
    def __init__(self, number_nodes=NUMBER_NODES, 
                       number_parallel_tasks=NUMBER_PARALLEL_TASKS,
                       dask_scheduler=None,
                       kafka_zk_hosts=None):
        self.number_nodes=number_nodes
        self.number_parallel_tasks=number_parallel_tasks
        self.kafka_zk_hosts=kafka_zk_hosts
        print self.kafka_zk_hosts
        # Init Dask
        #print "init dask: " + str(dask_scheduler)
        #self.dask_distributed_client = Client(dask_scheduler)   
        print "init local dask"
        self.dask_distributed_client = Client(processes=False)   
        #self.dask_distributed_client = Client(processes=True)   
        #print "end init dask"
   
    
    def clean_kafka(self):
        cmd="%s/bin/kafka-topics.sh --delete --zookeeper %s --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, TOPIC_NAME)
        print cmd
        #os.system(cmd)
        #time.sleep(60)
    
        cmd="%s/bin/kafka-topics.sh --create --zookeeper %s --replication-factor 1 --partitions %d --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, NUMBER_PARTITIONS, TOPIC_NAME)
        print cmd
        os.system(cmd)
    
        cmd="%s/bin/kafka-topics.sh --describe --zookeeper %s --topic %s"%(KAFKA_HOME, self.kafka_zk_hosts, TOPIC_NAME)
        print cmd
        os.system(cmd)
        
    
    
    def run(self):          
        #run_timestamp=datetime.datetime.now()
        #RESULT_FILE= "results/kafka-throughput-producer-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
#
        #try:
        #    os.makedirs("results")
        #except:
        #    pass
        #
        #output_file=open(RESULT_FILE, "w")
        #output_file.write("Number_Clusters,Number_Points_per_Cluster,Number_Dim,Number_Points_per_Message,Interval,Number_Partitions,Number_Processes,Number_Nodes,Time,Points_per_sec,Records_per_sec,Dask_Config\n")
        #bytes = 0
        #num_messages = 0
        #count = 0
        
        global_start = time.time()
        #for num_points_per_message in NUMBER_POINTS_PER_MESSAGE:
        num_points_per_message = NUMBER_POINTS_PER_MESSAGE
        #for idx, num_cluster in enumerate([NUMBER_CLUSTER]):
        num_cluster = NUMBER_CLUSTER
        count_produces = 0
        num_point_per_cluster = NUMBER_POINTS_PER_CLUSTER
        while count_produces < NUMBER_OF_PRODUCES:
            self.clean_kafka()
            start = time.time()
            # Using Dask Delay API
            tasks = []
            for block_id in range(NUMBER_PARALLEL_TASKS):
                print "Generate: " + str(block_id)
                #produce_block(block_id, self.kafka_zk_hosts)
                t = delayed(produce_block, pure=False)(block_id, self.kafka_zk_hosts)
                tasks.append(t)
                # {"block_id": block_id,   "kafka_zk_hosts":self.kafka_zk_hosts
                #                                       })
                
                # produce_block(block_id)
                #self.dask_distributed_client.submit(self.test)
                #self.dask_distributed_client.submit(self.test, self)
                
                #tasks.append(self.dask_distributed_client.submit(produce_block))
            
            print "Gather"
            #self.dask_distributed_client.gather(tasks)
            res = delayed(tasks).compute()
            print str(res)
            #delayed(t).compute()    
                
                #t = delayed(self.produce_block, pure=True)(self, block_id)
                #tasks.append(t)
            #delayed(tasks).compute()
            
            # Using Dask Bag API
            #bag = db.from_sequence([str(x) for x in range(NUMBER_PARALLEL_TASKS)],  npartitions=NUMBER_PARALLEL_TASKS)
            #bag.starmap(produce_block).compute()
            print "End Produce via Dask"
             
            end = time.time()
            print "Number: %d, Number Processes: %d, Time to produce %d points: %.1f"%(count_produces, NUMBER_PARALLEL_TASKS, 
                                                                                       num_cluster*num_point_per_cluster, end-
                                                                                       start)
           #output_file.write("%d,%d,%d,%d,%d,%d,%d,%d,%.5f,%.5f,%.5f,dask-distributed\n"%(num_cluster,num_point_per_cluster,NUMBER_DIM, 
           #                                               num_points_per_message,INTERVAL,NUMBER_PARTITIONS,NUMBER_PARALLEL_TASKS, NUMBER_NODES,
           #                                                          (end-start), ((num_cluster*num_point_per_cluster)/(end-start)),
           #                                                              (((num_cluster*num_point_per_cluster)/num_points_per_message)/(end-start))
           #                                                              ))
           #output_file.flush()
            count_produces = count_produces + 1
        
            #time.sleep(INTERVAL)
            
            #time.sleep(INTERVAL)
        
        #output_file.close()
        