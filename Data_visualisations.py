from pymongo.mongo_client import MongoClient
import matplotlib.pyplot as plt
import pandas as pd


class Viz:
    try:
        myclient = MongoClient(host="mongodb+srv://cluster0-jxbdj.mongodb.net/test",
                               username='haseeb',
                               password='123')
        db = myclient['case_study']
        print('connection to MongoDB was successful')
    except Exception:
        print("did not connect")

    def __init__(self):
        self.myclient = Viz.myclient
        self.db = Viz.db

    def A(self):
        try:
            self.col = self.db.ServiceArea
            pandf = pd.DataFrame(list(self.col.find()))
            state_count_df = pandf.groupby('StateCode').count()
            a = state_count_df[['ServiceAreaName', 'SourceName', 'BusinessYear']]
            a.plot.bar(figsize=(10, 10))
            plt.show()
        except Exception:
            print('''it looks like this data hasn't been loaded in Mongo Yet.
                     Please make sure that isn't the case''')

    def B(self):
        # The following plot shows the sourcename for the country.
        try:
            self.col = self.db["ServiceArea"]
            df = self.col.groupby('SourceName').count()
            sc = df[['SourceName', 'County']]
            sc.plot.bar(figsize=(10, 10))
            plt.show()
        except Exception:
            print('''it looks like this data hasn't been loaded in Mongo Yet.
                     Please make sure that isn't the case''')

    def D(self):
        try:
            self.col = self.db.BenefitsCostSharing
            pandf = pd.DataFrame(list(self.col.find()))
            b = pandf[['BenefitName', 'StateCode']].groupby(['StateCode'])['BenefitName'].count()
            b.plot.bar(figsize=(10, 10))
            plt.show()
        except Exception:
            print('''it looks like this data hasn't been loaded in Mongo Yet.
                     Please make sure that isn't the case''')

    def E(self):
        try:
            self.col = self.db.insurance
            data = pd.DataFrame(list(self.col.find()))
            abc = data.groupby('region').count()['_id']
            sm = data[data.smoker == 'yes'].groupby('region').count()['_id']
            xyz = sm / abc
            xyz.plot.bar(figsize=(10, 10))
            plt.show()
        except Exception:
            print('''it looks like this data hasn't been loaded in Mongo Yet.
                     Please make sure that isn't the case''')

    def F(self):
        '''Question D -- Benefit plans per state'''
        self.col = self.db.insurance
        pdf = pd.DataFrame(list(self.col.find()))
        abc = pdf.groupby('region').count()['_id']
        sm = pdf[pdf.smoker == 'yes'].groupby('region').count()['_id']
        xyz = sm / abc
        xyz.plot.bar(figsize=(10, 10))
        plt.show()


def main():
    V = Viz()
    V.F()


if __name__ == '__main__':
    main()
