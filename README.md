# cassandra_elasticsearch
Sincronizador simples entre Cassandra e Elasticsearch

Funcionamento:

Inicialmente a aplicação conecta e cria o keyspace e collumn families no Cassandra. Bem como o index e o doc_type no Elasticsearch.
Depois de feitas as conexões, eh lido todos os registros da collumn family 'tweet' no keyscpace do Cassandra. Para cada registro encontrado
verificado se existe o registro correspondente no Elasticsearch. Se o registro já existir, os dados são atualizados conforme o que
tiver a data de alteração mais recente. Se o registro ainda não estiver indexado no Elasticsearch o mesmo é inserido no index.

Depois que todos os registro do Cassandra são sincronizados com o Elasticsearch eh verificado quais registro que estão indexados
no Elasticsearch ainda não foram incluidos no Cassandra. Os registro que forem encontrados são inseridos no Cassandra.


Column family e doc_type:

Foi criado uma column family que representa tweets:

E o seu doc_type correspondente:


Diretórios

resource: diretório contento arquivo utilizados durante a execução do programa. Atualmente existe somente
o arquvo com o CQL utilizado para criar a base de dados no cassandra.
src: diretório com os fontes da aplicação


Considerações:

A aplicação considera que não existe o keyspace no Cassandra. Por isso, ele cria o keyspace e a collumn family utilizados

Run:

Para facilicar a execução foi criado um shell script no qual passa todos os parametros necessários para execução ( com valores default ). Que são:

-t: tempo entre cada sincronização
-r: diretório de resources da aplicação. Nesse diretório deve conter o .cql responsavel em criar o keyspace e a collumn family
-c: host para conectar com o Cassandra
-e: host de conexão com o Elasticsearch
