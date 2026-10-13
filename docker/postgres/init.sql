-- One Postgres instance, two fenced databases. Each role owns exactly
-- one database and has no rights on the other, so a cross-DB query fails
-- at the connection, not in code review.
CREATE USER order_user WITH PASSWORD 'order_pw';
CREATE USER product_user WITH PASSWORD 'product_pw';
CREATE DATABASE order_db OWNER order_user;
CREATE DATABASE product_db OWNER product_user;
-- PUBLIC holds CONNECT on every database by default, so revoking from the
-- role alone changes nothing. Revoke from PUBLIC, then grant back only to
-- the owner role.
REVOKE CONNECT ON DATABASE order_db FROM PUBLIC;
GRANT CONNECT ON DATABASE order_db TO order_user;
REVOKE CONNECT ON DATABASE product_db FROM PUBLIC;
GRANT CONNECT ON DATABASE product_db TO product_user;
