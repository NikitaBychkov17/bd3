import psycopg2 
from psycopg2.sql import SQL, Identifier

def create_db():
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()
    
    cur.execute("""
                DROP TABLE client_Phone;
                DROP TABLE clients;
                """)
    
    #Создание таблицы клиентов
    cur.execute("""CREATE TABLE IF NOT EXISTS clients (
                client_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                surname TEXT NOT NULL,
                email TEXT NOT NULL
                );""")
    
    cur.execute(""" CREATE TABLE IF NOT EXISTS client_Phone(
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients (client_id),
                phone VARCHAR(12)
                );""")
    
    conn.commit()
    conn.close()

def add_client(name, surname, email, phones = None):
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()

    cur.execute("""INSERT INTO clients (name, surname, email)
                VALUES (%s, %s, %s)
                RETURNING client_id, name, surname, email
                """, (name, surname, email))

    conn.commit()
    conn.close()
    
def add_phone(client_id, phone):
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()

    cur.execute("""INSERT INTO client_Phone(client_id, phone)
                VALUES(%s, %s)
                RETURNING client_id, phone;
                """, (client_id, phone))

    conn.commit()
    conn.close()
    
def update_client(client_id, name = None, surname = None, email = None):
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()
    arg_list = {'name': name, 'surname': surname, 'email': email}
    for key, arg in arg_list.items():
        if arg:
            cur.execute(SQL('UPDATE clients SET {}=%s WHERE client_id = %s').format(Identifier(key)), (arg,client_id))
                
    cur.execute("""SELECT * FROM clients
                WHERE client_id = %s;
                """, client_id)

    conn.commit()
    conn.close()
    
def delete_phone(client_id):
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()

    cur.execute("""DELETE FROM client_Phone
                WHERE client_id=%s
                RETURNING client_id
                """, (client_id,))
    
    conn.commit()
    conn.close()
    
def delete_client(client_id):
    delete_phone(client_id)
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()

    cur.execute("""DELETE FROM clients
                WHERE client_id = %s
                """, (client_id,))
        
    conn.commit()
    conn.close()
    
def find_client(name = None, surname = None, email = None, phone = None):
    conn = psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432')
    cur = conn.cursor()

    cur.execute("""SELECT * FROM clients c
                LEFT JOIN client_Phone p ON c.client_id = p.client_id
                WHERE (name = %(name)s OR %(name)s IS NULL)
                AND (surname = %(surname)s OR %(surname)s IS NULL)
                AND (email = %(email)s OR %(email)s IS NULL)
                OR (phone = %(phone)s OR %(phone)s IS NULL);
                """, {'name': name, 'surname': surname, 'email': email, 'phone': phone})
        
    
with psycopg2.connect(database='clients.db', user='postgres', password='1718LFIf_nik', port='5432') as conn:   
    
    create_db()

    # Добавление клиента
    add_client('Иван', 'Иванов', 'ivan@gmail.com')

    # Добавление телефона для клиента
    add_phone(1, '+79123456780')

    # Обновление данных клиента
    update_client('1', 'Пётр', 'Петров', 'petr@gmail.com')

    # Удаление телефона для клиента
    delete_phone('1')

    # Удаление клиента
    delete_client(1)

    # Поиск клиента
    find_client('Иван', 'Иванов', 'ivan@gmail.com', '+79123456780')
    
conn.close()