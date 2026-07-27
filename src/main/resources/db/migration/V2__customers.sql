CREATE TABLE customers (
    id          INTEGER PRIMARY KEY,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    email       VARCHAR(255),
    gender      VARCHAR(50),
    contact_no  VARCHAR(50),
    country     VARCHAR(255),
    dob         VARCHAR(32)
);

CREATE INDEX idx_customers_email ON customers (email);
