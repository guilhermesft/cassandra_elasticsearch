#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import getopt

from cassandra.cluster import Cluster

def main():
	cassandra = init_cassandra("/home/vanz/repos/cassandra_elasticsearch/resource")
	#TODO

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
	return session

def init_elasticsearch():
	pass

if __name__ == "__main__":
	main()
