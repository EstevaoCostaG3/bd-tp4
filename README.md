# bd-tp4
Banco de Dados - Trabalho Prático 4: 
 - Criação de um esquema relacional a partir de [dados de produtos da Amazon](https://snap.stanford.edu/data/amazon-meta.html). 
 - Parsing e povoamento dos dados no banco de dados criado a partir do esquema.
 - Realização de consultas sobre as relações

### Dependências (biblitecas para Pyhton 3)

 - psycopg2
 - simplejson
 - gzip
 - tqdm
 - terminaltables

### Usage

Clone o repositório e baixe os [dados de produtos da Amazon](https://snap.stanford.edu/data/amazon-meta.html). Mova o arquivo baixado para o diretório anterior ao diretório do repositório (`../bd-tp4`).

Parsing dos dados de produtos:
 
```python3 parser.py > ../results.json```

Criação do esquema no banco de dados `<nome-do-BD>` e povoamento das relações com os dados:

```python3 our_parser.py <endereço-do-servidor> <usuario> <senha> <nome-do-BD>```

Realização de consultas sobre as tabelas do banco de dados `<nome-do-BD>` criado anteriormente:

```python3 dashboard.py <endereço-do-servidor> <usuario> <senha> <nome-do-BD>```

### Consultas implementadas
 - Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação
 - Dado um produto, listar os produtos similares com mais vendas do que ele
 - Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no banco de dados
 - Listar os 10 produtos líderes de venda em cada grupo de produtos
 - Listar os 10 produtos com a maior média de avaliações úteis positivas
 - Listar a 5 categorias de produto com a maior média de avaliações úteis positivas
 - Listar os 10 clientes que mais fizeram comentários por grupo de produto

### Autoria
[Estevão Costa](https://github.com/EstevaoCostaG3)

[Vitor Xavier](https://github.com/xaveiro)
