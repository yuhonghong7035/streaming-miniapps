#!/bin/python
#
# /home/01131/tg804093/work/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1 --conf "spark.executor.memory=110g" --files ../saga_hadoop_utils.py KMeans_SparkStreamingThroughputConsumer.py
#
# With app log:
#  /home/01131/tg804093/work/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1 --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:///work/01131/tg804093/wrangler/spark-logs" --files ../saga_hadoop_utils.py  KMeans_SparkStreamingThroughputConsumer.py


import os
import sys
from subprocess import check_output
import time
import datetime
import logging
logging.basicConfig(level=logging.WARN)
import urllib
import json
import socket
import re
import pandas as pd
import subprocess
import binascii
import tempfile
import tomopy
import dxchange

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.1 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2:2.2.1'

import pyspark
from pyspark.streaming.listener import StreamingListener
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

from pykafka import KafkaClient

###############################################################################
# CONFIGURATIONS
# Get current cluster setup from work directory
KAFKA_HOME = "/home/01131/tg804093/work/kafka_2.11-1.0.0"
SPARK_HOME = "/home/01131/tg804093/work/spark-2.2.1-bin-hadoop2.7"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1"
SPARK_APP_PORT = 4040  # 4041 if ngrok is running
# SARK_MASTER="local[1]"
SPARK_LOCAL_IP = socket.gethostbyname(socket.gethostname())
TOPIC = 'Throughput'
NUMBER_EXECUTORS = 1
STREAMING_WINDOW = 10
SCENARIO = "16_Producer_Kmeans"
KAFKA_STREAM = "Direct"

###############################################################################

# Global info shared across all functions
run_timestamp = datetime.datetime.now()
spark_cores = 1
number_partitions = 1
model = None

global work_dir
work_dir = os.getcwd()


global reconstruction_algorithm
reconstruction_algorithm = "gridrec"


def get_number_partitions(kafka_zk_hosts, topic):
    try:
        cmd = KAFKA_HOME + \
            "/bin/kafka-topics.sh --describe --topic %s --zookeeper %s" % (topic, kafka_zk_hosts)
        print cmd
        out = check_output(cmd, shell=True)
        number = re.search("(?<=PartitionCount:)[0-9]*", out).group(0)
        return number
    except:
        return 0


def get_application_details(sc):
    try:
        app_id = sc.applicationId
        url = "http://" + SPARK_LOCAL_IP + ":" + \
            str(SPARK_APP_PORT) + "/api/v1/applications/" + app_id + "/executors"
        max_id = -1
        while True:
            cores = 0
            response = urllib.urlopen(url)
            data = json.loads(response.read())
            print data
            for i in data:
                print "Process %s" % i["id"]
                if i["id"] != "driver":
                    cores = cores + i["totalCores"]
                    if int(i["id"]) > max_id:
                        max_id = int(i["id"])
            print "Max_id: %d, Number Executors: %d" % (max_id, NUMBER_EXECUTORS)
            if (max_id == (NUMBER_EXECUTORS-1)):
                break
            time.sleep(.1)
            return cores
    except:
        pass
    return 0


def get_streaming_performance_details(app_id, filename):
    """curl http://localhost:4041/api/v1/applications/app-20170805185159-0004/streaming/batches"""
    url = "http://" + SPARK_LOCAL_IP + ":" + \
        str(SPARK_APP_PORT) + "/api/v1/applications/" + app_id + "/streaming/batches"
    response = urllib.urlopen(url)
    df = pd.read_json(response.read())
    df.to_csv(filename)


#######################################################################################
# Collecting Performance Information about batch throughput
class BatchInfoCollector(StreamingListener):

    def __init__(self):
        super(StreamingListener, self).__init__()
        self.batchInfosCompleted = []
        self.batchInfosStarted = []
        self.batchInfosSubmitted = []

    def onBatchSubmitted(self, batchSubmitted):
        self.batchInfosSubmitted.append(batchSubmitted.batchInfo())

    def onBatchStarted(self, batchStarted):
        self.batchInfosStarted.append(batchStarted.batchInfo())

    def onBatchCompleted(self, batchCompleted):
        global run_timestamp
        info = batchCompleted.batchInfo()
        submissionTime = datetime.datetime.fromtimestamp(info.submissionTime()/1000).isoformat()

        SPARK_RESULT_FILE = "results/spark-metrics-" + \
            run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
        with open(SPARK_RESULT_FILE, "a") as output_spark_metrics:
            output_spark_metrics.write("%s, %s, %d, %d, %d, %d,%s\n" % (str(info.batchTime()),
                                                                        submissionTime,
                                                                        info.schedulingDelay(),
                                                                        info.processingDelay(),
                                                                        info.totalDelay(),
                                                                        info.numRecords(), SCENARIO))
            output_spark_metrics.flush()
        self.batchInfosCompleted.append(batchCompleted.batchInfo())

    def onStreamingStarted(self, streamStarted):
        pass

    def onReceiverStarted(self, receiverStarted):
        """
        Called when a receiver has been started
        """
        pass

    def onReceiverError(self, receiverError):
        """
        Called when a receiver has reported an error
        """
        pass

    def onReceiverStopped(self, receiverStopped):
        """
        Called when a receiver has been stopped
        """
        pass

    def onOutputOperationStarted(self, outputOperationStarted):
        """
        Called when processing of a job of a batch has started.
        """
        pass

    def onOutputOperationCompleted(self, outputOperationCompleted):
        """
        Called when processing of a job of a batch has completed
        """
        pass


def printOffsetRanges(rdd):
    for o in offsetRanges:
        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)


######################################################################################
# different process functions

def count_records(rdd):
    count = rdd.count()
    return count


def pre_process(datetime, rdd):
    # print (str(type(time)) + " " + str(type(rdd)))
    global run_timestamp
    start = time.time()
    points = rdd.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
    end_preproc = time.time()
    count = -1  # points.count()
    end_count = time.time()
    RESULT_FILE = "results/spark-pre_process-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
    with open(RESULT_FILE, "a") as output_file:
        output_file.write("Points PreProcess, %d, %d, %s, %.5f\n" %
                          (spark_cores, count, number_partitions, end_preproc-start))
        output_file.write("Points Count, %d, %d, %s, %.5f\n" %
                          (spark_cores, count, number_partitions, end_count-end_preproc))
        output_file.flush()
    return points
    # points.pprint()
    # model.trainOn(points)


def model_update(rdd):
    global run_timestamp
    count = -1  # rdd.count()
    start = time.time()
    lastest_model = model.latestModel()
    lastest_model.update(rdd, decayFactor, timeUnit)
    end_train = time.time()
    # predictions=model.predictOn(points)
    # end_pred = time.time()
    RESULT_FILE = "results/spark-model_update-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
    with open(RESULT_FILE, "a") as output_file:
        output_file.write("KMeans Model Update, %d, %d, %s, %.5f\n" %
                          (spark_cores, count, number_partitions, end_train-start))
        output_file.flush()
    # output_file.write("KMeans Prediction, %.3f\n"%(end_pred-end_train))
    # return predictions


def model_prediction(rdd):
    global run_timestamp
    count = -1  # rdd.count()
    start = time.time()
    lastest_model = model.latestModel()
    print "Call Predict On:"
    model_output = lastest_model.predictOn(rdd)
    end = time.time()
    RESULT_FILE = "results/spark-model_update-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
    with open(RESULT_FILE, "a") as output_file:
        output_file.write("KMeans Model Inference, %d, %d, %s, %.5f\n" %
                          (spark_cores, count, number_partitions, end-start))
        output_file.flush()
    return model_output

###############################################################################
# LightSource

 def lightsource_reconstruction(message):
    global run_timestamp
    global work_dir
    print type(message), len(message), len(message[1])
    count = -1  # rdd.count()
    start = time.time()
    reconstruct(message[1])
    end_train = time.time()

    RESULT_FILE_LIGHT = os.path.join(
        work_dir, "results/spark-light-recon-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv")
    with open(RESULT_FILE_LIGHT, "a") as output_file:
        output_file.write("Light Recon, %d, %d, %s, %.5f\n" %
                          (spark_cores, count, number_partitions, end_train-start))
        output_file.flush()
    return (end_train-start)


def reconstruct(message):
    global reconstruction_algorithm
    start = 0
    end = 2
    # msg_bin = base64.b64decode(message)
    msg_bin = binascii.unhexlify(message)
    tf = tempfile.NamedTemporaryFile(delete=True)
    tf.write(msg_bin)
    tf.flush()
    proj, flat, dark, theta = dxchange.read_aps_32id(tf.name, sino=(start, end))
    theta = tomopy.angles(proj.shape[0])
    proj = tomopy.normalize(proj, flat, dark)
    rot_center = tomopy.find_center(proj, theta, init=290, ind=0, tol=0.5)
    proj = tomopy.minus_log(proj)
    recon = tomopy.recon(proj, theta, center=rot_center, algorithm=reconstruction_algorithm)
    recon = tomopy.circ_mask(recon, axis=0, ratio=0.95)
    # plt.imsave("/home/01131/tg804093/notebooks/streaming-miniapps/recon.png", recon[0, :,:], cmap='Greys_r')
    # plt.savefig()
    tf.close()
    return True

###############################################################################
# PeakFinder

def PeakFinder_Analysis(message):

    import workflows.main_local_dask as main_local_dask ## fix pythonpath

    global timestamp
    global work_dir
    print type(message), len(message), len(message[1])
    count = -1  # rdd.count()
    start = time.time()

    msg_bin = binascii.unhexlify(message)
    data_folder = dict()  #TODO: remove from scistreams
    data_folder['data_folder'] = 'marked_for_deleltion'

    tf = tempfile.NamedTemporaryFile(delete=True)
    tf.write(msg_bin)
    tf.flush()

    analyze = main_local_dask.primary_func(data_folder,[tf.name])
    tf.close()

    end_train = time.time()
    RESULT_FILE_PEAK = os.path.join(
    work_dir, "results/spark-PeakFinder" + \
    run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv")

    with open(RESULT_FILE_PEAK, "a") as output_file:
        output_file.write("PeakFinder, %d, %d, %s, %.5f\n" %
                    (spark_cores, count, number_partitions, end_train-start))
        output_file.flush()

    return (end_train - start)





##########################################################################################################################
# Start Streaming App

class MiniApp(object):

    def __init__(
        self,
        spark_master=None,
        kafka_zk_hosts=None,
        number_parallel_tasks=1,
        number_clusters=1,  # kmeans
        number_dim=3,  # kmeans
        topic_name="test",
        application_type="kmeans",  # kmeans or light or PeakFinder
        streaming_window=60,  # streaming window in sec
        test_scenario=SCENARIO,
        application="kmeans"
    ):

        # Spark
        self.spark_master = spark_master
        self.spark_context = None
        # Kafka
        self.topic_name = topic_name
        self.kafka_zk_hosts = kafka_zk_hosts
        self.kafka_client = KafkaClient(zookeeper_hosts=self.kafka_zk_hosts)
        self.kafka_brokers = ",".join(["%s:%d" % (i.host, i.port)
                                       for i in self.kafka_client.brokers.values()])
        print self.kafka_brokers
        self.number_partitions = get_number_partitions(self.kafka_zk_hosts, self.topic_name)
        self.scenario = test_scenario
        self.application = application
        self.reconstruction_algorithm = "gridrec"

        if self.application.find("-") > 0:
            self.reconstruction_algorithm = self.application.split("-")[1]
            self.application = self.application.split("-")[0]
        global reconstruction_algorithm
        reconstruction_algorithm = self.reconstruction_algorithm
        self.number_clusters = int(number_clusters)
        global SCENARIO
        SCENARIO = self.scenario
        self.streaming_window = int(streaming_window)
        print "Streaming Window: %d sec" % self.streaming_window

    def start(self):
        #######################################################################################
        # Init Spark
        start = time.time()
        self.spark_context = pyspark.SparkContext(master=self.spark_master,
                                                  appName="Streaming MASA")
        ssc_start = time.time()
        ssc = StreamingContext(self.spark_context, self.streaming_window)
        batch_collector = BatchInfoCollector()
        ssc.addStreamingListener(batch_collector)

        # make basic cluster information globally available
        global spark_cores
        global number_partitions
        spark_cores = get_application_details(self.spark_context)
        number_partitions = self.number_partitions

        # print str(sc.parallelize([2,3]).collect())

        #######################################################################################
        # Logging
        try:
            os.makedirs("results")
        except:
            pass
        RESULT_FILE = "results/spark-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
        SPARK_RESULT_FILE = "results/spark-metrics-" + \
            run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
        output_spark_metrics = open(SPARK_RESULT_FILE, "w")
        output_spark_metrics.write(
            "BatchTime, SubmissionTime, SchedulingDelay, ProcessingDelay, TotalDelay, NumberRecords, Scenario\n")
        output_spark_metrics.flush()
        output_file = open(RESULT_FILE, "w")
        output_file.write("Measurement,Spark Cores,Number Points,Number_Partitions, Time\n")
        output_file.write("Spark Startup, %d, %d, %s, %.5f\n" %
                          (spark_cores, -1, self.number_partitions, time.time()-start))
        output_file.flush()

        print "Topic: %s, Number Partitions: %s, Spark Master:%s, Kafka Broker: %s Clusters: %d" % (self.topic_name,
                                                                                                    str(
                                                                                                        self.number_partitions),
                                                                                                    self.spark_master,
                                                                                                    self.kafka_brokers,
                                                                                                    self.number_clusters)

        fromOffset = {}
        for i in range(int(self.number_partitions)):
            topicPartion = TopicAndPartition(self.topic_name, i)
            fromOffset = {topicPartion: long(0)}

        #######################################################################################
        # Spark Helper
        kafka_dstream = None
        if KAFKA_STREAM == "Direct":
            print "Use Direct Kafka Connection"
            kafka_dstream = KafkaUtils.createDirectStream(ssc, [self.topic_name],
                                                          kafkaParams={"metadata.broker.list": self.kafka_brokers,
                                                                       "auto.offset.reset": "smallest",
                                                                       "heartbeat.interval.ms": "3600000",
                                                                       "session.timeout.ms": "3600000",
                                                                       "fetch.message.max.bytes": "10000000"})
        elif KAFKA_STREAM == "Receiver":
            print "Use Receiver Kafka Connection"
            kafka_dstream = KafkaUtils.createStream(ssc, self.kafka_zk_hosts,
                                                    "spark-streaming-consumer",
                                                    {self.topic_name: 1})

        # kafka_param: "metadata.broker.list": brokers
        #              "auto.offset.reset" : "smallest" # start from beginning
        # kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST,
        #                                                             "auto.offset.reset" : "smallest"}) #, fromOffsets=fromOffset)
        ssc_end = time.time()
        output_file.write("Spark SSC Startup, %d, %d, %s, %.5f\n" % (spark_cores, -1,
                                                                     self.number_partitions,
                                                                     ssc_end-ssc_start))

        #####################################################################
        # Scenario Count

        # global counts
        # counts=[]
        # kafka_dstream.foreachRDD(lambda t, rdd: counts.append(rdd.count()))

        #####################################################################
        # Scenario KMeans
        #######################################################################################
        # KMeans Functions
        global decayFactor
        decayFactor = 1.0
        global timeUnit
        timeUnit = "batches"
        global max_value
        max_value = 0
        global model

        model = StreamingKMeans(k=self.number_clusters, decayFactor=decayFactor,
                                timeUnit=timeUnit).setRandomCenters(3, 1.0, 0)

        if self.application == "kmeans" or self.application == "kmeansstatic":
            print "kmeans"
            points = kafka_dstream.transform(pre_process)
            # points.foreachRDD(model_update)
            model.update(points, 0.0, u"batches")
        elif self.application == "kmeanspred" or self.application == "kmeansstaticpred":
            print "kmeanspred"
            points = kafka_dstream.transform(pre_process)
            # model.trainOn(points, 0.0, u"batches")
            model.trainOn(points)
            m = model.predictOn(points)
            # m=points.foreachRDD(model_prediction)
            if m is not None:
                m.pprint()
            else:
                print "Result None"
        elif self.application == "PeakFinder":
            print "PeakFinder Analysis"
            res = kafka_dstream.map(PeakFinder_Analysis)
            res.pprint()
        else:
            print "Light Reconstruction"
            # rdd = kafka_dstream.transform(lightsource_reconstruction)
            res = kafka_dstream.map(lightsource_reconstruction)
            res.pprint()
            # rdd.pprint()
        ssc.start()
        ssc.awaitTermination()
        ssc.stop(stopSparkContext=True, stopGraceFully=True)
        output_file.close()
        output_spark_metrics.close()

    def run(self):
        cmd = self._get_spark_command()
        check_output(cmd, shell=True)

    def run_in_background(self):
        cmd = self._get_spark_command()
        # check_output(cmd, shell=True)
        self.spark_process = subprocess.Popen(cmd, shell=True)

    def _get_spark_command(self):
        filename = os.path.abspath(__file__)
        if filename[-1] == "c":
            filename = filename[:-1]  # remove c from "spark.pyc"
        # pkg_resources.resource_filename(module, "tooth.h5")
        cmd = """spark-submit --master %s --packages %s --conf 'spark.executor.memory=110g' %s %s %s %s %s %d %s %d""" % (self.spark_master, SPARK_KAFKA_PACKAGE, filename,
                                                                                                                          self.spark_master, self.kafka_zk_hosts, self.topic_name,
                                                                                                                          self.scenario, 60, self.application + "-" + self.reconstruction_algorithm, self.number_clusters)
        print cmd
        return cmd

    def cancel(self):
        self.spark_process.kill()


if __name__ == "__main__":
    print(str(sys.argv))

    spark_details = {'master_url': sys.argv[1]}
    kafka_details = {'master_url': sys.argv[2]}
    mini = MiniApp(
        spark_master=spark_details["master_url"],
        kafka_zk_hosts=kafka_details["master_url"],
        topic_name=sys.argv[3],
        test_scenario=sys.argv[4],
        streaming_window=sys.argv[5],
        application=sys.argv[6],
        number_clusters=int(sys.argv[7])
    )
    mini.start()


#####################################################################
# Other

# points = kafka_dstream.transform(pre_process)
# points.count().pprint()
# print "********Found %d *************"%sum(counts)

# global count_messages
# count_messages  = sum(counts)
#
# output_file.write(str(counts))
# kafka_dstream.count().pprint()

# print str(counts)
# count = kafka_dstream.count().reduce(lambda a, b: a+b).foreachRDD(lambda a: a.count())
# if count==None:
#    count=0
# print "Number of Records: %d"%count


# predictions=model_update(points)
# predictions.pprint()


# We create a model with random clusters and specify the number of clusters to find
# model = StreamingKMeans(k=10, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
# points=kafka_dstream.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
# points.pprint()


# Now register the streams for training and testing and start the job,
# printing the predicted cluster assignments on new data points as they arrive.
# model.trainOn(trainingStream)
# result = model.predictOnValues(testingStream.map(lambda point: ))
# result.pprint()

# Word Count
# lines = kvs.map(lambda x: x[1])
# counts = lines.flatMap(lambda line: line.split(" ")) \
#        .map(lambda word: (word, 1)) \
#        .reduceByKey(lambda a, b: a+b)
# counts.pprint()
