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

def main(wait_time, resource_dir, c_host, e_host):
	#conecta no cassandra e no elastic search
	cassandra = init_cassandra(resource_dir, [c_host])
	es = init_elasticsearch([e_host])
	#loop da aplicação. Ela nunca para até ser forcada a parada
	while(True):
		#faz a sincronia
		sync(cassandra, es)
		#espera o tempo entre cada sync
		time.sleep(wait_time)

def init_cassandra(resource_dir, seeds = ["127.0.0.1"]):
	"""
	Método responsavel em conectar e inicializar a base da dados no Cassandra
	resource_dir: diretórios contendo os resources utilizados pela aplicação
	seeds: seeds para conexão com o cassandra cluster
	"""
	#conectar na base
	cluster = Cluster(seeds)
	session = cluster.connect()
	#cria a database
	with open(resource_dir + "/init.cql", "r") as schema_init:
		for line in schema_init:
			session.execute(line)
	#configura o keyspace correto da aplicação
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

def sync(cassandra, es):
	"""
	Método responsável pelo processo de sincronia própriamente dito.
	cassandra: conexão com o cassandra
	es: conexão com o elastic search
	"""
	#pega todos os registros do cassandra
	registros = cassandra.execute("SELECT id, user, content, retweet, date, likes, last_update FROM tweets")
	ids = []
	print "Registro no cassandra = " + str(len(registros))
	for registro in registros:
		#guarda id para verificar depois com o es
		ids.append(str(registro.id))
		# verifica se esse id ja existe no elastic search
		if es.exists(index='simbiose', doc_type='tweet', id=registro.id):
			#registro ja existe no es, então precisa ser sincronizado
			syncRecord(registro, cassandra)
		else:
			#registro não existe no es. Precisa ser inserido
			insertIntoEs(registro, es)
	# depois que sincronizou todos os registros existentes no cassandra no es, traz o que tem de
	# novo no es para o cassandra
	s = Tweet.search().query(~Q("ids", type="tweet", values=ids)).extra(from_=0, size=9999)
	response = s.execute()
	for record in response:
		print "registro vindo do es= " + str(record)
		insertIntoCassandra(record, cassandra)

def syncRecord(registro, cassandra):
	"""
	Método que sincroniza registros entre o Cassandra e o Elasticsearch

	"""
	#recupera o registro do es
	es_tweet = Tweet.get(id=str(registro.id))
	#verifica se o registro do Cassandra eh mais novo do que o que esta no es
	if es_tweet.last_update < registro.last_update:
		#o registro do Cassandra eh mais recente. Atualiza o registro do es
		tweet = Tweet(user=registro.user, content=registro.content, retweet=registro.retweet, date=registro.date, likes=registro.likes, last_update=registro.last_update)
		tweet.meta.id=registro.id
		tweet.save()
	else:
		# o registro do es eh mais recente. Atualiza o registro do Cassandra
		cassandra.execute("""
			UPDATE tweets SET user = %(user)s, content = %(content)s, retweet = %(retweet)s, date = %(date)s, likes = %(likes)s, last_update = %(last_update)s
			WHERE id =  %(id)s
		""",
		{'id' : uuid.UUID(es_tweet.meta.id), 'user' : es_tweet.user, 'content' : es_tweet.content, 'retweet' : es_tweet.retweet, 'date' : es_tweet.date, 'likes' : es_tweet.likes, 'last_update' : es_tweet.last_update})

def insertIntoEs(registro, es):
	"""
	Método para inserir um registro no index do elastic search
	registro: registro recuperado do cassandra
	es: conexão com o elastic search
	"""
	tweet = Tweet(user=registro.user, content=registro.content, retweet=registro.retweet, date=registro.date, likes=registro.likes, last_update=registro.last_update)
	tweet.meta.id=registro.id
	tweet.save()

def insertIntoCassandra(record, cassandra):
	"""
	Método para inserir um registro da column family do cassandra
	record: registro recuperado do elastic search
	cassandra: conexão com o cassandra
	"""
	cassandra.execute("""
		INSERT INTO tweets (id, user, content, retweet, date, likes, last_update)
		VALUES (%(id)s, %(user)s, %(content)s, %(retweet)s, %(date)s, %(likes)s, %(last_update)s)
		""",
		{'id' : uuid.UUID(record.meta.id), 'user' : record.user, 'content' : record.content, 'retweet' : record.retweet, 'date' : record.date, 'likes' : record.likes, 'last_update' : record.last_update})


def usage():
	print "sync.py -t <integer> -r <diretório de resources> -c <Cassandra host> -e <Elasticsearch host>"
	print "Parâmetros:"
	print "-t: tempo entre cada sincronização"
	print "-r: path para o diretório de resouce"
	print "-c: host para conexão com o Cassandra"
	print "-e: host para conexão com o Elasticsearch"


if __name__ == "__main__":
	try:
		#valida e faz parse de todos os parâmetros
		options, args = getopt.getopt(sys.argv[1:], 't:r:c:e:')
		if len(options) == 4:
			sync_time = int(options[0][1]) #tempo entre cada sincronia
			resource_dir = options[1][1] #diretório de resouce
			cassandra_host = None #host Cassandra
			es_host = None #host Elasticsearch
			for opt in options:
				prefix = opt[0]
				if prefix == '-t':
					sync_time = int(opt[1])
				elif prefix == '-r':
					resource_dir = opt[1]
				elif prefix == '-c':
					cassandra_host = opt[1]
				elif prefix == '-e':
					es_host = opt[1]
			if sync_time >= 0:
				main(sync_time, resource_dir, cassandra_host, es_host)
		else:
			usage()
	except Exception as err:
		import traceback
		traceback.print_exc()
		sys.exit(1)
	sys.exit(0)
