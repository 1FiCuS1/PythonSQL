import psycopg2


def create_db(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(200) NOT NULL,
            last_name VARCHAR(200) NOT NULL,
            email VARCHAR(50) UNIQUE NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS phones (
            id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES clients(id),
            phone VARCHAR(20) NOT NULL
        );
        """
    )
    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    if first_name is None or last_name is None or email is None:
        print("Одно или несколько полей не заполнено: Имя, Фамилия, Email")
        return

    cur.execute(
        """
        INSERT INTO clients (first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING id, first_name, last_name;
        """,
        (first_name, last_name, email),
    )
    new_client = cur.fetchone()
    if phones:
        cur.execute(
            """
            INSERT INTO phones (client_id, phone) VALUES (%s, %s);
            """,
            (new_client[0], phones),
        )
        print(f"Клиент {new_client} добавлен")


def get_phones(cur, client_id, phones):
    cur.execute(
        """
        SELECT phone FROM phones WHERE client_id=%s AND phones=%s;
        """,
        (client_id, phones),
    )
    found_phone = cur.fetchall()
    return found_phone


def add_phone(conn, client_id, phones):
    found_phone = get_phones(cur, client_id, phones)
    if found_phone is None or len(found_phone) == 0:
        print("Телефон не может быть пустым")
    cur.execute(
        """
                INSERT INTO phones(client_id, phones) VALUES (%s, %s);
                """,
        (client_id, phones),
    )
    conn.commit()


def change_client(
    conn, client_id, first_name=None, last_name=None, email=None, phones=None
):
    if first_name is not None:
        cur.execute(
            """
                    UPDATE clients SET first_name = %s WHERE id = %s;
                    """,
            (first_name, client_id),
        )
    if last_name is not None:
        cur.execute(
            """
                    UPDATE clients SET last_name = %s WHERE id = %s;
                    """,
            (last_name, client_id),
        )
    if email is not None:
        cur.execute(
            """
                    UPDATE clients SET email = %s WHERE id = %s;
                    """,
            (email, client_id),
        )
    if phones is not None:
        cur.execute(
            """
                    UPDATE phones SET phones = %s WHERE client_id = %s;
                    """,
            (phones, client_id),
        )
    cur.execute(
        """
                SELECT * FROM clients;
                """
    )
    conn.commit()


def delete_phone(conn, client_id, phones):
    cur.execute(
        """
                    DELETE FROM phones WHERE client_id = %s AND phones = %s;
                    """,
        (client_id, phones),
    )
    cur.execute(
        """
                    SELECT * FROM phones WHERE  client_id = %s;
                    """,
        (client_id,),
    )
    print(cur.fetchall())


def delete_client(conn, client_id):
    cur.execute(
        """
                DELETE FROM phones WHERE client_id = %s;
                """,
        (client_id,),
    )
    cur.execute(
        """
                DELETE FROM clients WHERE id = %s;
                """,
        (client_id,),
    )
    cur.execute(
        """
                SELECT * FROM clients;
                """
    )
    print(cur.fetchall())


def find_client(conn, first_name=None, last_name=None, email=None, phones=None):
    if first_name is None:
        first_name = "%"
    else:
        first_name = "%" + first_name + "%"
    if last_name is None:
        last_name = "%"
    else:
        last_name = "%" + last_name + "%"
    if email is None:
        email = "%"
    else:
        email = "%" + email + "%"
    if phones is None:
        cur.execute(
            """
                    SELECT c.client_id, c.first_name, c.last_name, c.email, p.phones FROM clients c
                    LEFT JOIN phones p ON c.client_id =p.client_id
                    WHERE c.first_name LIKE %s AND c.last_name LIKE %s AND c.email LIKE %s;
                    """,
            (first_name, last_name, email, phones),
        )
    else:
        cur.execute(
            """
                    SELECT c.client_id, c.first_name, c.last_name, c.email, p.phones FROM clients c
                    LEFT JOIN phones p ON c.client_id =p.client_id
                    WHERE c.first_name LIKE %s AND c.last_name LIKE %s AND c.email LIKE %s AND p.phones LIKE %s;
                    """,
            (first_name, last_name, email, phones),
        )
    return cur.fetchall()


def all_clients(cur):
    cur.execute(
        """
            SELECT * FROM clients;
            """
    )
    print(cur.fetchall())
    cur.execute(
        """
            SELECT * FROM phones;
            """
    )
    print(cur.fetchall())


if __name__ == "__main__":
    with psycopg2.connect(
        database="PythonSQL", user="postgres", password="postgres"
    ) as conn:
        with conn.cursor() as cur:
            create_db(conn, cur)

            add_client(conn, "Сергей", "Петров", "sergey@bk.ru", "89011234567")
            add_client(conn, "Иван", "Иванов", "ivan@bk.ru", "89021234567")
            add_client(conn, "Петр", "Петров", "petr@bk.ru", "89031234567")
            add_client(conn, "Анна", "Сергеева", "anna@bk.ru", "89041234567")
            add_client(conn, "Юрий", "Антонов", "anton@bk.ru", "89051234567")

            all_clients(cur)

            add_phone(conn, 1, "89111234567")
            add_phone(conn, 1, "89121234567")
            add_phone(conn, 2, "89131234567")
            add_phone(conn, 3, "89141234567")
            add_phone(conn, 4, "89151234567")

            all_clients(cur)

            change_client(conn, cur, 1, first_name="Денис")
            change_client(conn, cur, 2, None, "Пупкина")
            change_client(conn, cur, 3, None, None, "gitar@gmail.com")
            change_client(conn, cur, 2, None, None, None, "+79594457396")

            all_clients(cur)

            delete_phone(conn, 1, "89111234567")
            delete_phone(conn, 1, "89121234567")

            all_clients(cur)

            delete_client(conn, 1)
            delete_client(conn, 2)
            delete_client(conn, 3)
            delete_client(conn, 4)
            delete_client(conn, 5)

            all_clients(cur)

            find_client(
                cur, first_name="Мария", last_name="Алексеева", email="dgfsg@bk.ru"
            )

            delete_client(conn, 3)

            all_clients(cur)
