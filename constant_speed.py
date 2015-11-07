# -*- coding: utf-8 -*-
from mongo_helper import MongoConn
from time import strftime,localtime
import MySQLdb
import sys
import ConfigParser

mongo = None

def mongo_init():
	global mongo
	servers,database = load()
	print("servers:%s database:%s"%(servers,database))
	mongo = MongoConn(servers,database)
	print("aaaaaaaaa")
	mongo.connect()
	mongo.set_db()
	

def mongo_operation(connection,param,method):
	try:
		global mongo
		mongo.set_connection(connection)
		if method == 'update':
			mongo.insert(param)
		elif method == 'delete':
			mongo.delete(param)
	except Exception, e:
		raise e
	
def monitor_params(method):
	try:
		time=strftime('%Y%m%d%H',localtime())+'0000'
		pv = 2L
		price = 1.0
		timeType = 1
		dataType = "2"
		planIds = get_planId()
		param = {}
		param['time'] = long(time)
		param['pv'] = pv
		param['price'] = price
		param['timeType'] =timeType
		param['dataType'] = dataType
		for planId in planIds:
			print("planId:%s"%planId)
			param['planId'] = planId
			mongo_operation('ssp_show_monitor',param,method)
			print("method:%s param:%s"%(method,param))
	except Exception, e:
		raise e
	
def report_params(method):
	try:
		param = {}
		param['showPv'] = 30
		param['productNo'] = 2
		param['placeChannel'] = 3
		param['menu2'] = '客户端网游'
		param['menu1'] = '游戏'
		param['clickPv'] = 0
		param['showPrice'] = "0"
		param['baId'] =  "280050"
		param['measureId'] = "0002"
		param['aderId'] ="13603933"
		param['price'] = 0
		param['platformType'] = 2
		param['appActivationPv'] = 0
		param['appDownloadPv'] = 0
		param['exportPv'] = 8
		param['appOpenPv'] = 0
		param['dataType'] =  1
		param['uv'] = 0
		param['aderType'] = 1
		placeIds = get_placeId()
		for placeId in placeIds:
			param['placeId'] = placeId
			for x in xrange(1,3):
				timeType = x
				param['timeType'] = timeType
				if timeType == 1:
					time=strftime('%Y%m%d%H',localtime())+'0000'
				else:
					time=strftime('%Y%m%d',localtime())+'000000'
				param['time'] = long(time)
				mongo_operation('ssp_media_report',param,method)
				print("method:%s param:%s"%(method,param))
	except Exception, e:
		raise e

def get_placeId():
	try:
		conn=MySQLdb.connect(host='10.99.31.12',user='game',passwd='game',db='adp_feed_admin',port=3326)
		sql = "select position_id from adp_feed_unit where unit_name like '%press_test_%' group by position_id"
		cur=conn.cursor()
		cur.execute(sql)
		postion_id = cur.fetchall()
		cur.close()
		conn.close()
		return postion_id
	except Exception, e:
		raise e
	
def get_planId():
	try:
		conn=MySQLdb.connect(host='10.99.31.12',user='game',passwd='game',db='adp_feed_admin',port=3326)
		sql = "select plan_id from adp_feed_unit where unit_name like '%press_test_%' "
		cur=conn.cursor()
		cur.execute(sql)
		plan_id = cur.fetchall()
		cur.close()
		conn.close()
		return plan_id
	except Exception, e:
		raise e
	


		

#加载配置
def load():
	try:
		servers  = ""
		database = ""
		cf = ConfigParser.ConfigParser()
		cf.read("config.conf")
		mongo_host = cf.get("mongo", "host")
		mongo_port = cf.get("mongo", "port")
		mongo_db   = cf.get("mongo", "database")
		mongo_user = cf.get("mongo","user")
		mongo_pass = cf.get("mongo","pass")
		if mongo_user=='' or mongo_pass=='':
			servers = 'mongodb://'+mongo_host+':'+mongo_port+'/'
			database = mongo_db
		else:
			servers = 'mongodb://'+mongo_user+':'+mongo_pass+'@'+mongo_host+':'+mongo_port+'/'+mongo_db
		return servers,database
	except Exception, e:
		raise e
				

def main():
	if len(sys.argv) < 2:
		sys.exit()
	method = sys.argv[1]
	mongo_init()
	monitor_params(method)
	report_params(method)
	mongo.close()

def test():
	global mongo
	mongo_init()
	mongo.set_connection('ssp_media_report')
	results = mongo.find({'placeId':'1444287077112'})
	for result in results:
		print("result:%s"%result)


if __name__ == '__main__':
	test()
	#main()
