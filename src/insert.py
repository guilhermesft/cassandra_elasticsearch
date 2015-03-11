#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import getopt
import uuid
import time
from sets import Set
from datetime import datetime

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, DocType, String, Date, Integer
from elasticsearch_dsl.connections import connections

class Tweet(DocType):
	"""
	Classe utilizada para mapear o doc_type utilizado no Elasticsearch
	"""
	user = String()
	content = String()
	retweet = Integer()
	date = Date()
	likes = Integer()
	last_update = Date()

	class Meta:
		#index utilizado pela aplicação
		index = 'simbiose'

def main( c_host, e_host):
	#conecta no cassandra e no elastic search
	cassandra = init_cassandra( [c_host])
	es = init_elasticsearch([e_host])
	for r in range(100):
		#insere no Elasticsearch
		tweet = Tweet(user='user_' + str(r), content='content_' + str(r), retweet=r, date=datetime.now(), likes=r, last_update=datetime.now())
		tweet.meta.id=uuid.uuid1()
		tweet.save()
		#insere no Cassandra
		cassandra.execute("""
		INSERT INTO tweets (id, user, content, retweet, date, likes, last_update)
		VALUES (%(id)s, %(user)s, %(content)s, %(retweet)s, %(date)s, %(likes)s, %(last_update)s)
		""",
		{'id' : uuid.uuid1(), 'user' : 'user_c_' + str(r), 'content' : 'content_c_' + str(r), 'retweet' : r, 'date' : datetime.now(), 'likes' : r, 'last_update' : datetime.now()})

def init_cassandra(seeds = ["127.0.0.1"]):
	"""
	Método responsavel em conectar e inicializar a base da dados no Cassandra
	seeds: seeds para conexão com o cassandra cluster
	"""
	#conectar na base
	cluster = Cluster(seeds)
	session = cluster.connect()
	#cria a database
	session.set_keyspace('simbiose')
	return session

def init_elasticsearch(seeds=['127.0.0.1'], port=9200):
	"""
	Método responsavel em conectar ao elastic search cluster.
	seeds: list do host utilizados para conectar no cluster
	port: port utilizada para se conectar no cluster
	"""
	# conecta com o Elasticsearch
	connections.add_connection('default', Elasticsearch(seeds, port = port))
	#inicio o index e o doc_type
	Tweet.init()
	return connections.get_connection()

def usage():
	print "insert.py  -c <Cassandra host> -e <Elasticsearch host>"
	print "Parâmetros:"
	print "-c: host para conexão com o Cassandra"
	print "-e: host para conexão com o Elasticsearch"

if __name__ == "__main__":
	try:
		#valida e faz parse de todos os parâmetros
		options, args = getopt.getopt(sys.argv[1:], 'c:e:')
		if len(options) == 2:
			cassandra_host = None #host Cassandra
			es_host = None #host Elasticsearch
			for opt in options:
				prefix = opt[0]
				if prefix == '-c':
					cassandra_host = opt[1]
				elif prefix == '-e':
					es_host = opt[1]
			main( cassandra_host, es_host)
		else:
			usage()
	except Exception as err:
		import traceback
		traceback.print_exc()
		sys.exit(1)
	sys.exit(0)
