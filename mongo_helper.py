#encoding=utf-8
import pymongo
import sys


#遗留问题，每个对象都会加载配置，这应该不对
class MongoConn(object):
	"""操作MongoDb"""
	conn = None
	def __init__(self,servers,database):
		self.servers  = servers
		self.database = database

	#连接Mongo
	def connect(self):
		try:
			if self.conn is None:
				self.conn = pymongo.Connection(self.servers)
		except Exception, e:
			raise e
		

	#关闭Mongo连接
	def close(self):
		try:
			if self.conn:
		   	   self.conn.disconnect()
		except Exception, e:
			raise e
		

	#列出server info
	def server_info(self):
		print self.conn.server_info()

	#列出全部数据库
	def show_dbs(self):
		print self.conn.database_names()

	#选择数据库
	def set_db(self):
		try:
			self.db=self.conn[self.database]
		except Exception, e:
			raise e
		
	#选择集合
	def set_connection(self,table):
		try:
			self.collection = self.db[table]
		except Exception, e:
			raise e
		

	#添加数据到集合中
	def insert(self,param):
		try:
			self.collection.insert(param)
		except Exception, e:
			raise e

	#删除记录
	def delete(self,param):
		try:
			self.collection.remove(param)
		except Exception, e:
			print("error")
			raise e

	#查询单条记录
	def find_one(self,param=''):
		try:
			if param=='':
				data = self.collection.find_one()
			else:
				data = self.collection.find_one(param)
			return data
		except Exception, e:
			raise e

	#查询多条记录
	def find(self,param=''):
		try:
			if param=='':
				data = self.collection.find()
			else:
				data = self.collection.find(param)
			return data
		except Exception, e:
			raise e

	





			
