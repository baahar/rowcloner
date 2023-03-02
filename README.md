# rowcloner

The code works with PostgreSQL databases. One to pull the data from and one to clone the data to. 

You can use the following schema to create example tables in both.

CREATE TABLE company ( 
  id SERIAL PRIMARY KEY,
  name TEXT  
);

CREATE TABLE login ( 
  id SERIAL PRIMARY KEY,
  email TEXT
);

CREATE TABLE client (
  id SERIAL PRIMARY KEY,
  name TEXT, 
  address TEXT,
  company_id int REFERENCES company( id ),
  login_id int REFERENCES login( id ),
  referred_by int REFERENCES client( id )
);
