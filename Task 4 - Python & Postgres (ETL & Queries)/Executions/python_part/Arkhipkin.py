from pyodbc import Error
import pyodbc
import datetime

server = "localhost"
port = "5432"
db_source = "source"
db_target = "target"

#input credentials for postgres accounts
login_source = "postgres"
password_source = "password"
login_target = "postgres"
password_target = "password"


def get_postgres_conection_string(server, port, db, login, password):
    connection_string = (
    "DRIVER={PostgreSQL Unicode};"
    "DATABASE=" + db + ";"
    "UID=" + login + ";"
    "PWD=" + password + ";"
    "SERVER=" + server + ";"
    "PORT=" + port + ";"
    )
    return connection_string


conn_str_source = get_postgres_conection_string(server, port, db_source, login_source, password_source)
conn_str_target = get_postgres_conection_string(server, port, db_target, login_target, password_target)


def get_db_connection(connection_string):
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Error as e:
        print(e.args[1])
        return None


def execute_sql(conn, sql_query):
    try:
        c = conn.cursor()
        c.execute(sql_query)
        c.commit()
    except Error as e:
        print(e)


def get_one_value_from_db(db_connection, sql_query):
    result = None
    conn = db_connection
    if conn:
        crsr = conn.execute(sql_query)
        result = crsr.fetchone()
        crsr.close()
    return result

def get_last_import_date(connection_target):

    sql_select = """
        select 
                import_date
            from
                (select
                        import_date
                    from public.address
                union all
                select 
                        import_date
                    from public.company
                ) tabl
            order by import_date desc
            limit 1
        """
    last_import = get_one_value_from_db(connection_target, sql_select)
    return last_import


def create_connection_target():
    """ create a database connection to database """
    conn_target = get_db_connection(conn_str_target)
    last_import_date = None
    if conn_target:
        try:
            last_import_date = get_last_import_date(conn_target)
        except Error as e:
            # create target tables if not exists
            create_table_query = """
                CREATE TABLE IF NOT EXISTS address
                    (
                        id integer not null unique,
                        company_id text COLLATE pg_catalog."default",
                        country character varying(255) COLLATE pg_catalog."default",
                        postal_code character varying(255) COLLATE pg_catalog."default",
                        city character varying(255) COLLATE pg_catalog."default",
                        district character varying(255) COLLATE pg_catalog."default",
                        street character varying(255) COLLATE pg_catalog."default",
                        street_number character varying(255) COLLATE pg_catalog."default",
                        addition character varying(255) COLLATE pg_catalog."default",
                        created_at date,
                        import_date date,
                        unique (id, company_id, country, postal_code, city, district, street, street_number, addition, created_at)
                    );
                CREATE TABLE IF NOT EXISTS company
                    (
                        company_id text COLLATE pg_catalog."default" not null unique,
                        status character varying(255) COLLATE pg_catalog."default",
                        rating_threshold character varying(255) COLLATE pg_catalog."default",
                        company_name text COLLATE pg_catalog."default",
                        foundation_date date,
                        legal_form character varying(255) COLLATE pg_catalog."default",
                        created_at date,
                        import_date date,
                        unique (company_id, status, rating_threshold, company_name, foundation_date, legal_form, created_at)
                    );    
                    """
            execute_sql(conn_target, create_table_query)
    return conn_target, last_import_date


conn_target, last_import_date = create_connection_target()
if last_import_date is None:
    last_import_date = [datetime.date(2010, 1, 1)]
last_import_date_str = last_import_date[0].strftime('%Y-%m-%d')
print(conn_target, last_import_date_str )


def get_new_data_from_source():
    conn_source = get_db_connection(conn_str_source)
    addresses = []
    companies = []
    if conn_source:
        sql_select = """
            select 
                    *
                from public.address
                where created_at >= '""" + last_import_date_str +"'"
        cur = conn_source.cursor()
        cur.execute(sql_select)
        addresses = cur.fetchall()
        conn_source.commit()

        sql_select = """
            select 
                    *
                from public.company
                where created_at >= '""" + last_import_date_str +"'"
        cur = conn_source.cursor()
        cur.execute(sql_select)
        companies = cur.fetchall()
        conn_source.commit()

        conn_source.close()
    return addresses, companies

addresses, companies = get_new_data_from_source()

def insert_new_data_into_targer(addresses, companies):
    if conn_target:
        if addresses:
            for address in addresses:
                insert_row = tuple([item for item in address]) + (datetime.date.today(),)
                try:
                    c = conn_target.cursor()
                    c.execute(
                        """insert into address (id, company_id, country, postal_code, city, district, street, street_number, addition, created_at, import_date) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                        , insert_row
                    )
                    conn_target.commit()
                except Error as e:
                    if e.args[0] == '23505':
                        print("Insert line ignored because of duplicate")
                    else:
                        print("Catched an unexpexted error during insert try")

        if companies:
            for company in companies:
                insert_row = tuple([item for item in company]) + (datetime.date.today(),)
                try:
                    c = conn_target.cursor()
                    c.execute(
                        """insert into company (company_id, status, rating_threshold, company_name, foundation_date, legal_form, created_at, import_date) values (?, ?, ?, ?, ?, ?, ?, ?)"""
                        , insert_row
                    )
                    conn_target.commit()
                except Error as e:
                    if e.args[0] == '23505':
                        print("Insert line ignored because of duplicate")
                    else:
                        print("Catched an unexpexted error during insert try")


insert_new_data_into_targer(addresses, companies)
if conn_target:
    conn_target.close()


def simple_consistency_test():
    target_addresses_count = None
    target_companies_count = None
    source_addresses_count = None
    source_companies_count = None

    conn_target = get_db_connection(conn_str_target)
    if conn_target:
        sql_select = "select count(*) from public.address"
        target_addresses_count = get_one_value_from_db(conn_target, sql_select)
        sql_select = "select count(*) from public.company"
        target_companies_count = get_one_value_from_db(conn_target, sql_select)
        conn_target.close()

    conn_source = get_db_connection(conn_str_source)
    if conn_source:
        sql_select = "select count(*) from public.address"
        source_addresses_count = get_one_value_from_db(conn_source, sql_select)
        sql_select = "select count(*) from public.company"
        source_companies_count = get_one_value_from_db(conn_source, sql_select)
        conn_source.close()

    if (source_companies_count or source_addresses_count) and (target_companies_count or target_addresses_count):
        if source_companies_count == target_companies_count and source_addresses_count == target_addresses_count:
            print('The same rows counts in the source and target databases')
        else:
            print('Alert: different rows counts in the source and target databases!!!')

simple_consistency_test()