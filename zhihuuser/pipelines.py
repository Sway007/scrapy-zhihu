# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


from scrapy.exporters import JsonItemExporter
import json

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
        
        user_name = item.get('name')
        if user_name not in self.users_to_exporter:
            data = dict(item)
            f = open('{}/{}.json'.format(self.dir, user_name), 'w')
            json.dump(data, f, ensure_ascii=False, indent=4)
        return item
        
    