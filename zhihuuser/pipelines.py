# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


from scrapy.exporters import JsonItemExporter
import json
from pymongo import MongoClient

class ZhihuuserPipeline(object):
    def __init__(self):
        self.dir = 'users'

    def open_spider(self, spider):
        self.users_to_exporter = {} 

    def close_spider(self, spider):
        for exporter in self.users_to_exporter.values():
            exporter.finish_exporting()
            exporter.file.close()

    def _exporter_for_item(self, item):
        user_name = item.get('name')
        if user_name not in self.users_to_exporter:
            f = open('{}/{}.json'.format(self.dir, user_name), 'w+b')
            exporter = JsonItemExporter(f)
            exporter.start_exporting()
            self.users_to_exporter[user_name] = exporter
        return self.users_to_exporter[user_name]

    def process_item(self, item, spider):   
        exporter = self._exporter_for_item(item)
        exporter.export_item(item)
        return item


class customPipline(object):
    def __init__(self, count):
        self.dir = 'users'
        self.count = count

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('TEST_LIMIT'))

    def open_spider(self, spider):
        self.users_to_exporter = {} 

    def close_spider(self, spider):
        for file in self.users_to_exporter.values():
            file.close()

    def process_item(self, item, spider):   
        # input('custom pipline!')
        user_name = item.get('name')
        if user_name not in self.users_to_exporter:
            data = dict(item)
            f = open('{}/{}.json'.format(self.dir, user_name), 'w')
            json.dump(data, f, ensure_ascii=False, indent=4)
        return item
        
class mongodbPipeline(object):

    def __init__(self, count, dbserver_ip, database, collection):
        self.count = count
        self.db_ip = dbserver_ip
        self.database = database
        self.collection = collection

    @classmethod
    def from_crawler(cls, crawler):
        return cls( crawler.settings.get('TEST_LIMIT'), 
            crawler.settings.get('MONGODB_SERVER_IP'),
            crawler.settings.get('MONGODB_DATABASE'),
            crawler.settings.get('MONGODB_COLLECTION'))

    def open_spider(self, spider):
        self.client = MongoClient(self.db_ip)
        self.db = self.client[self.database]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):  
        # input('mongodb pipline!') 
        self.db[self.collection].update({'url_token': item['url_token']}, {'$set': dict(item)}, upsert=True)
        return item