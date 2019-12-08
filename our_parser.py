import simplejson as json
import psycopg2
import re
import sys
from tqdm import tqdm

def get_product_sql(content):
	sql = "INSERT INTO PRODUCT "
	columns = ['id', 'title', 'ASIN', 'group', 'salesrank']
	columns_to_insert = ''
	values_to_insert = ''
	for column in columns:
		if(column in content):
			columns_to_insert += ', ' + column
			if(column != 'id' and column != 'salesrank'):
				if(column == 'title'):
					values_to_insert += ", '" + content[column].replace("'", "''") + "'"
				else:
					values_to_insert += ", '" + content[column] + "'"
			else:
				values_to_insert += ', ' + str(content[column])
	sql = sql + '(' + columns_to_insert[1:] + ') VALUES (' + values_to_insert[1:] + ')'
	return sql.replace('group', '_group')

def get_product_category_sql(content):
	if('categories' not in content):
		return []
	commands = []
	for category_hierarchy in content['categories']:
		category = re.search('\d+\]$', category_hierarchy)
		if(category != None):
			category = category.group()[:-1]
			sql = "INSERT INTO PRODUCT_CATEGORY (product_id, category_id) VALUES ( " + content['id'] +", " + category +")"
			commands.append(sql)
	return commands

customer_ids = {}

def get_customer_sql(content):
	if('reviews' not in content):
		return []
	reviews = content['reviews']
	sql_commands = []
	for review in reviews:
		if(review['customer_id'] in customer_ids):
			continue
		customer_id = str(review['customer_id'])

		customer_ids[customer_id] = 1
		sql = "INSERT INTO CUSTOMER (id) VALUES ('" + customer_id +"')"
		sql_commands.append(sql)
	return sql_commands

def get_product_similar_sql(content):
	if('similar_items' not in content):
		return []
	similar_items = content['similar_items']
	sql_commands = []
	for  similar in similar_items:
		sql = "INSERT INTO PRODUCT_SIMILAR (similar_asin, product_id) VALUES ('" + similar +"', '" + content['id'] + "')"
		sql_commands.append(sql)
	return sql_commands

def get_reviews_sql(content):
	if('reviews' not in content):
		return []
	reviews = content['reviews']
	sql_commands = []
	columns = ['rating', 'votes', '_date', 'helpful', 'customer_id']
	
	for review in reviews:
		columns_to_insert = ''
		values_to_insert = ''
		for column in columns:
			if(column in review):
				columns_to_insert += ', ' + column
				if(column == '_date' or column == 'customer_id'):
					values_to_insert += ", '" + str(review[column]) + "'"
				else:
					values_to_insert += ', ' + str(review[column])
		sql = "INSERT INTO REVIEW ( product_id " + columns_to_insert + ") VALUES (" + content['id'] + values_to_insert + ")"
		sql_commands.append(sql)
	return sql_commands

category_ids = {}

def get_category_sql(content):
	if('categories' not in content):
		return []
	all_categories = content['categories']
	commands = []
	for category_hierarchy in all_categories:
		categories = category_hierarchy.split("|")[1:]
		for i in range(0, len(categories)):
			temp = categories[i].split("[")
			category_id = temp[-1][:-1]
			if(category_id in category_ids):
				continue
			category_ids[category_id] = 1
			name = temp[0]
			if(i == 0):
				parent_id = "NULL"
			else:
				parent_id = categories[i - 1].split("[")[1][:-1]
			sql = "INSERT INTO CATEGORY (id, name, parent_id) VALUES ("+ category_id +", '" + name.replace("'", r"''") + "', " + parent_id +" )"
			commands.append(sql)
	return commands


def generate_inserts():
	file_path = "../results.json"
	with open(file_path) as f:
		lines = f.readlines()
		for line in tqdm(lines[1:], total=len(lines)-1):
			content = json.loads(line)
			
			yield get_product_sql(content)

			for command in get_category_sql(content):
				yield command

			for command in get_customer_sql(content):
				yield command

			for command in get_reviews_sql(content):
				yield command

			for command in get_product_category_sql(content):
				yield command

			for command in get_product_similar_sql(content):
				yield command

if __name__ == '__main__':

	if(len(sys.argv) != 5):
		print("Erro! Este programa deve ser executado assim:")
		print("python3 our_parser.py <endereço-do-servidor> <usuario> <senha> <nome-do-BD>")
		sys.exit(1)

	try:
		host = sys.argv[1]
		user = sys.argv[2]
		password = sys.argv[3]
		database = sys.argv[4]
		con = psycopg2.connect(host=host, database=database, user=user, password=password)
		cur = con.cursor()
		cur.execute("DROP TABLE IF EXISTS PRODUCT")
		cur.execute("CREATE TABLE PRODUCT(title VARCHAR(500), id INT NOT NULL, ASIN CHAR(10) NOT NULL, _group VARCHAR(15), salesrank INT, PRIMARY KEY (id),UNIQUE (ASIN))")

		cur.execute("DROP TABLE IF EXISTS CUSTOMER")
		cur.execute("CREATE TABLE CUSTOMER(id CHAR(14) NOT NULL, PRIMARY KEY (id))")

		cur.execute("DROP TABLE IF EXISTS PRODUCT_SIMILAR")
		cur.execute("CREATE TABLE PRODUCT_SIMILAR(similar_asin CHAR(10) NOT NULL, product_id INT NOT NULL, PRIMARY KEY (similar_asin, product_id), FOREIGN KEY (product_id) REFERENCES PRODUCT(id) )")

		cur.execute("DROP TABLE IF EXISTS  REVIEW")
		cur.execute("CREATE TABLE REVIEW(rating INT,votes INT,_date Date, helpful INT, product_id INT NOT NULL, customer_id CHAR(14) NOT NULL,FOREIGN KEY (product_id) REFERENCES PRODUCT(id),FOREIGN KEY (customer_id) REFERENCES CUSTOMER(id))")

		cur.execute("DROP TABLE IF EXISTS  CATEGORY")
		cur.execute("CREATE TABLE CATEGORY(id INT NOT NULL,name VARCHAR(100),parent_id INT,PRIMARY KEY (id),FOREIGN KEY (parent_id) REFERENCES CATEGORY(id))")

		cur.execute("DROP TABLE IF EXISTS PRODUCT_CATEGORY")
		cur.execute("CREATE TABLE PRODUCT_CATEGORY(product_id INT NOT NULL,category_id INT NOT NULL,PRIMARY KEY (product_id, category_id),FOREIGN KEY (product_id) REFERENCES PRODUCT(id),FOREIGN KEY (category_id) REFERENCES CATEGORY(id))")
		for command in generate_inserts():
			if(command):
				cur.execute(command)

		con.commit()
	except psycopg2.DatabaseError as e:
		if(con):
		    con.rollback()
		print(f'Error {e}')
		sys.exit(1)

	finally:
		if(con):
			con.close()
