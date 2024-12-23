information_schemaCREATE TABLE Производители (
    brand_id INT PRIMARY KEY AUTO_INCREMENT,
    brand_name VARCHAR(255) NOT NULL
);

CREATE TABLE Телефоны (
    phone_id INT PRIMARY KEY AUTO_INCREMENT,
    model_name VARCHAR(255) NOT NULL,
    brand_id INT,
    price DECIMAL(10, 2) NOT NULL,
    specs TEXT,
    FOREIGN KEY (brand_id) REFERENCES Производители(brand_id)
);

CREATE TABLE Клиенты (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone_number VARCHAR(15)
);

CREATE TABLE Заказы (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    order_date DATE NOT NULL,
    customer_id INT,
    FOREIGN KEY (customer_id) REFERENCES Клиенты(customer_id)
);

CREATE TABLE Детали_заказа (
    order_detail_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    phone_id INT,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES Заказы(order_id),
    FOREIGN KEY (phone_id) REFERENCES Телефоны(phone_id)
);
