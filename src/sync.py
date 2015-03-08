#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import uuid
from sets import Set
from datetime import datetime

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, DocType, String, Date, Integer
from elasticsearch_dsl.connections import connections

class Tweet(DocType):
	user = String()
	content = String()
	retweet = Integer()
	date = Date()
	likes = Integer()

	class Meta:
		index = 'simbiose'

def main():
	#conecta no cassandra e no elastic search
	cassandra = init_cassandra("/home/vanz/repos/cassandra_elasticsearch/resource")
	es = init_elasticsearch()
	#faz a sincronia
	#sync(cassandra, es)

def init_cassandra(resource_dir, seeds = ["127.0.0.1"]):
	"""
	Método responsavel em conectar e inicializar a base da dados no Cassandra
	resource_dir: diretórios contendo os resources utilizados pela aplicação
	seeds: seeds para conexão com o cassandra cluster
	"""
	#conectar na base
	cluster = Cluster(seeds)
	session = cluster.connect()
	with open(resource_dir + "/init.cql", "r") as schema_init:
		for line in schema_init:
			session.execute(line)
	session.set_keyspace('simbiose')
	return session

def init_elasticsearch(seeds=['127.0.0.1'], port=9200):
	"""
	Método responsavel em conectar ao elastic search cluster.
	seeds: list do host utilizados para conectar no cluster
	port: port utilizada para se conectar no cluster
	"""
	connections.add_connection('default', Elasticsearch(seeds, port = port))
	Tweet.init()
	return connections.get_connection()

def sync(cassandra, es):
	"""
	Método responsável pelo processo de sincronia própriamente dito.
	cassandra: conexão com o cassandra
	es: conexão com o elastic search
	"""
	#pega todos os registros do cassandra
	registros = cassandra.execute("SELECT id, user, content, retweet, date, likes FROM tweets")
	ids = []
	for registro in registros:
		#guarda id para verificar depois com o es
		ids.append(registro.id)
		# verifica se esse id ja existe no elastic search
		if es.exists(index='simbiose', doc_type='tweet', id=registro.id):
			syncWithEs(registro, es)
		else:
			insertIntoEs(registro, es)
	# depois que sincronizou todos os registros existentes no cassandra no es, traz o que tem de
	# novo no es para o cassandra
	s = Tweet.search().query(~Q("ids", type="tweet", values=ids))
	response = s.execute()
	for record in response:
		insertIntoCassandra(record, cassandra)

def syncWithEs(registro, es):
	pass

def insertIntoEs(registro, es):
	"""
	Método para inserir um registro no index do elastic search
	registro: registro recuperado do cassandra
	es: conexão com o elastic search
	"""
	tweet = Tweet(user=registro.user, content=registro.content, retweet=registro.retweet, date=registro.date, likes=registro.likes)
	tweet.meta.id=registro.id
	tweet.save()

def insertIntoCassandra(record, cassandra):
	"""
	Método para inserir um registro da column family do cassandra
	record: registro recuperado do elastic search
	cassandra: conexão com o cassandra
	"""
	cassandra.execute("""
		INSERT INTO tweets (id, user, content, retweet, date, likes)
		VALUES (%(id)s, %(user)s, %(content)s, %(retweet)s, %(date)s, %(likes)s)
		""",
		{'id' : record.id, 'user' : record.user, 'content' : record.content, 'retweet' : record.retweet, 'date' : record.date, 'likes' : record.likes})

if __name__ == "__main__":
	main()
