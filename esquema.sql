CREATE TABLE PRODUCT
(
  title VARCHAR(500),
  id INT NOT NULL,
  ASIN CHAR(10) NOT NULL,
  _group VARCHAR(15),
  salesrank INT,
  PRIMARY KEY (id),
  UNIQUE (ASIN)
);

CREATE TABLE CUSTOMER
(
  id CHAR(14) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE PRODUCT_SIMILAR
(
  similar_asin CHAR(10) NOT NULL,
  product_id INT NOT NULL,
  PRIMARY KEY (similar_asin, product_id),
  FOREIGN KEY (product_id) REFERENCES PRODUCT(id)
);

CREATE TABLE REVIEW
(
  rating INT,
  votes INT,
  _date Date,
  helpful INT,
  product_id INT NOT NULL,
  customer_id CHAR(14) NOT NULL,
  FOREIGN KEY (product_id) REFERENCES PRODUCT(id),
  FOREIGN KEY (customer_id) REFERENCES CUSTOMER(id)
);

CREATE TABLE CATEGORY
(
  id INT NOT NULL,
  name VARCHAR(100),
  parent_id INT,
  PRIMARY KEY (id),
  FOREIGN KEY (parent_id) REFERENCES CATEGORY(id)
);

CREATE TABLE PRODUCT_CATEGORY
(
  product_id INT NOT NULL,
  category_id INT NOT NULL,
  PRIMARY KEY (product_id, category_id),
  FOREIGN KEY (product_id) REFERENCES PRODUCT(id),
  FOREIGN KEY (category_id) REFERENCES CATEGORY(id)
);