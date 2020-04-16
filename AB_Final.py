from pyspark.sql import SparkSession
import os


# for topic: print(os.path.basename(url).split('_')[0])

class RDBMS:
    '''This class creates both a spark session and mongo spark session'''
    spark = SparkSession.builder.appName('Part_A').master('local[*]')\
        .config('spark.mongodb.output.uri', "mongodb+srv://cluster0-jxbdj.mongodb.net/test")\
        .getOrCreate()
#    spark = SparkSession.builder.config('spark.mongodb.output.uri', "mongodb://localhost").getOrCreate()

    def __init__(self, table_name):
        ''' takes in self, and pulls data out of SQL and returns an raw data'''
        self.table_name = table_name
        self.raw_table_data = RDBMS.spark.read.format('jdbc') \
            .options(url="jdbc:mysql://localhost:3306/cdw_sapp",
                     driver='com.mysql.cj.jdbc.Driver',
                     dbtable=table_name,
                     user='root',
                     password='').load()
        self.temp_viewDf = self.raw_table_data.createOrReplaceTempView(f'{table_name}')
        print('*'*10, f'{self} created')

    def __str__(self):
        return str(f"{self.table_name}_obj")

    def transform_data(self):
        '''takes in an object and returns a respectively transformed df'''

        print('*' * 15, f'extracting {self.table_name} data')

        if self.table_name == 'cdw_sapp_branch':
            self.tranformeddf = self.spark.sql(f'''
                                            SELECT BRANCH_CODE,
                                            BRANCH_NAME,
                                            BRANCH_STREET,
                                            BRANCH_CITY,
                                            BRANCH_STATE,
                                            IF(ISNULL(BRANCH_ZIP),'999999',BRANCH_ZIP) AS BRANCH_ZIP,
                                            CONCAT('(',SUBSTRING(BRANCH_PHONE, 1, 3),')-', SUBSTRING(BRANCH_PHONE, 4, 3),'-',SUBSTRING(BRANCH_PHONE, 7, 4)) as BRANCH_PHONE,
                                            LAST_UPDATED
                                            FROM {self.table_name}''')

        elif self.table_name == 'cdw_sapp_creditcard':
            self.tranformeddf = self.spark.sql(f'''
                                            SELECT TRANSACTION_ID,
                                            CONCAT(LPAD(DAY, 2, 0),
                                            LPAD(MONTH, 2, 0),YEAR) as TIMEID,
                                            CREDIT_CARD_NO,
                                            CUST_SSN,
                                            BRANCH_CODE,
                                            TANSACTION_TYPE,
                                            TRANSACTION_VALUE
                                            from {self.table_name}''')

        elif self.table_name == 'cdw_sapp_customer':
            self.tranformeddf = self.spark.sql(f'''
                                            SELECT SSN,
                                            initcap(FIRST_NAME) AS FIRST_NAME,
                                            lower(MIDDLE_NAME) AS MIDDLE_NAME,
                                            initcap(LAST_NAME) as LAST_NAME,
                                            CREDIT_CARD_NO,
                                            CONCAT(APT_NO,' ',STREET_NAME) AS CUST_STREET,
                                            CUST_CITY,
                                            CUST_STATE,
                                            CUST_COUNTRY,
                                            CUST_ZIP,
                                            CONCAT(SUBSTRING(CUST_PHONE, 1, 3),'-',SUBSTRING(CUST_PHONE,4,4)) AS CUST_PHONE,
                                            CUST_EMAIL,
                                            LAST_UPDATED
                                            FROM {self.table_name}''')

        del self.raw_table_data
        del self.temp_viewDf
        print('*' * 20, f'{self.table_name} extraction successful')
        return self

    def sendtoMongo(self):
        '''takes in a SqlTransfer object and sends it to the localhost'''

        try:
            self.tranformeddf.write.format('com.mongodb.spark.sql.DefaultSource')\
                .mode('append')\
                .option('database', 'case_study')\
                .option('collection', self.table_name).save()
            print('*' * 20, f'{self.table_name} is filed within DB', '*' * 20)
            self.tranformeddf = 'deleted'
        except Exception:
            print(f'could not connect with MongoDB')


def main():
    pass
    # table_names = ['cdw_sapp_branch','cdw_sapp_creditcard','cdw_sapp_customer']
    # for table in table_names:
    #     t = RDBMS(table)
    #     t.transform_data().sendtoMongo()
    # help(RDBMS)


if __name__ == '__main__':
    main()
