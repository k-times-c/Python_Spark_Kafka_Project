from pyspark.sql import SparkSession
import os
import requests
from kafka import KafkaProducer

class HIMT:
    parent_URL ='https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/'
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12-2.4.4 pyspark-shell' # spark-sql-kafka-0-10_2.11
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell' # giving original error;; changes the 2.4.1 to 2.4.4
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell' # giving original error;; changes the 2.4.1 to 2.4.4

    # /Users/KXC/anaconda3/lib/python3.7/site-packages/pyspark/jars/spark-streaming-kafka-0-10_2.11-2.4.4.jar

    spark = SparkSession.builder.config('spark.mongodb.output.uri',
                                        "mongodb+srv://cluster0-jxbdj.mongodb.net/test").getOrCreate()

    def __init__(self, base_list):
        self.parent_URL = HIMT.parent_URL
        if not isinstance(base_list, list):
            base_list = [base_list]

        self.topic_data_dict = {basename.split('.')[0]: requests.get(os.path.join(self.parent_URL + basename)) for basename in base_list}

        self.collection_name = next(iter(self.topic_data_dict)).split('_')[0]
        header_row = next(iter((next(iter(self.topic_data_dict.values())).text.splitlines())))
        self.delimiter = ',' if ',' in header_row else '\t'
        self.schema = header_row.split(self.delimiter)

        print("*"*50, f'\n instance {self} has been created! \n', "*"*50)
        print(f'collection name: {self.collection_name}',
              f'dictionary: {self.topic_data_dict}',
              f'schema: {self.schema}', sep='\n\t')

    '''the HIMT object has a context manager property to ensure it flushes upon completion or error'''
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.producer.flush()

    def __str__(self):
        return (f'{self.collection_name} Data Obj')

    @staticmethod
    def replace_excess_commas_in(line):
        if 'DentalGuard Preferred Group & Individual PPO, On & Off WV' in line or "22446, 22535, 22538, 22546, 22580" in line or "DentalGuard Preferred Group & Individual PPO, On & Off WV" in line:
            quote_split_line = line.split('"')[1].replace(',', ' ')
            new_line = ''.join([line.split('"')[0], '"', quote_split_line, line.split('"')[2]])
            return new_line

    def publish_subscribe_and_sink(self):
        '''
        this method publishes data to a Kafka topic. Method uses
        context manager to ensure producer is always "flushes" properly
        '''
        self.producer = HIMT.producer
        self.spark = HIMT.spark
        for topic_name, response in self.topic_data_dict.items():
            print("*" * 30, '\n', topic_name, response, '\n', "*" * 30)
            for idx, line in enumerate(response.text.splitlines()):

                if idx == 0:
                    continue  # Skipping over header row

                if idx == 5:
                    print('\n\n------- Producing lines to Kafka -------------')

                '''edge case logic, for carrier names in quotations'''
                if self.delimiter == ',':
                    if len(line.split(self.delimiter)) > len(self.schema): 
                        new_line = HIMT.replace_excess_commas_in(line)

                        try:
                            self.producer.send(topic_name, new_line.encode('utf-8'))
                        except Exception:
                            pass
                        continue
                self.producer.send(topic_name, line.encode('utf-8'))

            print(f'''...subscribing  to {topic_name} topic''')

            raw_kafka_df = self.spark.readStream.format("kafka")\
                .option("kafka.bootstrap.servers", "localhost:9092")\
                .option("subscribe", topic_name).option("startingOffsets", "earliest").load()
            kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
            output_query = kafka_value_df.writeStream.queryName(topic_name)\
                .format("memory").start()

            output_query.awaitTermination(10)
            value_df = self.spark.sql(f"select * from {topic_name}")
            print('\n', 'MOVING VALUE_RDD')

            if self.delimiter == '\t':
                value_rdd = value_df.rdd.map(lambda i: i['value'].split('\t'))
            else:
                value_rdd = value_df.rdd.map(lambda i: i['value'].split(','))

            df = self.spark.createDataFrame(value_rdd, schema=self.schema)
            print("\n\n", "*"*50, f'\nDF OBJ, "{df}", CREATED', end='\n\n')

            print('*'*100, f'''\n
            attempting to send df to {self.collection_name}''')

            try:
                df.write.format('com.mongodb.spark.sql.DefaultSource')\
                    .mode('append')\
                    .option('database', 'case_study')\
                    .option('collection', self.collection_name)\
                    .save()
                print(f'{topic_name} dataframe migration successful')
            except Exception:
                print('data transfer to MongoDB was unsuccessful')

def main():
    benefitsdata_URLbasenames = [
        'BenefitsCostSharing_partOne.txt',
        'BenefitsCostSharing_partTwo.txt',
        'BenefitsCostSharing_partThree.txt',
        'BenefitsCostSharing_partFour.txt'
        ]
    networkcsv_URLbasename = ['Network.csv']
    serviceArea_URLbasename = ['ServiceArea.csv']
    ins_URLbasename = ['insurance.txt']
    planAttributestxt_URLbasename = ['PlanAttributes.csv']

    URLs = [  #list of lists
        networkcsv_URLbasename,
        benefitsdata_URLbasenames,
        ins_URLbasename,
        serviceArea_URLbasename,
        planAttributestxt_URLbasename
        ]

    for idx, basename in enumerate(URLs):
        with HIMT(basename) as I:
            I.publish_subscribe_and_sink()
            print(f'{idx} of {len(URLs)} done.')
#    for HIMT_obj in DAOs: print(str(HIMT_obj),end='')
    print('All sent successfully')


if __name__ == '__main__':
    main()
