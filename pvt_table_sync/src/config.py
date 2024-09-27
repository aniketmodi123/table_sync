from fastapi import HTTPException, status
import psycopg2
import os
import redis
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from mysql import connector
from psycopg2.extras import RealDictCursor
import gspread
from google.oauth2.service_account import Credentials


PROD_HOST = os.environ.get('PRODUCTION_POSTGRES_HOST')
PROD_USER = os.environ.get('PRODUCTION_POSTGRES_USER')
PROD_PASSWORD = os.environ.get('PRODUCTION_POSTGRES_PASSWORD')
PROD_DB = os.environ.get('PRODUCTION_POSTGRES_DB')
PROD_PORT = os.environ.get('PRODUCTION_POSTGRES_PORT')

LEGECY_HOST = os.environ.get('MYSQL_HOST')
LEGECY_USER = os.environ.get('MYSQL_USER')
LEGECY_PASSWORD = os.environ.get('MYSQL_PASSWORD')


engine = create_engine(
    f'postgresql+psycopg2://{PROD_USER}:{PROD_PASSWORD}@{PROD_HOST}:{PROD_PORT}/{PROD_DB}'
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


# Create an engine with connection pooling for Legecy DB
legecy_engine = create_engine(
    f'mysql+mysqlconnector://{LEGECY_USER}:{LEGECY_PASSWORD}@{LEGECY_HOST}',
    pool_size=10, max_overflow=20
)

# Create a session factory
SessionLegecy = sessionmaker(autocommit=False, bind=legecy_engine)


# Create a session factory
SessionLegecy = sessionmaker(autocommit=False, bind=legecy_engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def psql_cursor(query, get='', put=''):
    con = psycopg2.connect(
        host=PROD_HOST,
        user=PROD_USER,
        password=PROD_PASSWORD,
        database=PROD_DB,
        port=PROD_PORT
    )

    cursor = con.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)

    try:
        if put:
            con.commit()
        elif get == 'one':
            return cursor.fetchone()
        else:
            return cursor.fetchall()
    except Exception as e:
        print(f'Query did not executed due to :--> {e}')
    finally:
        cursor.close()



def get_postgres_result(query):
    result = psql_cursor(query)
    if not result:
        raise Exception(status_code=status.HTTP_204_NO_CONTENT, detail="No data found")
    return result




def get_set_db_data(
    query, get='', put='', meny='', meny_data='', is_dict=True, data_value = {},
    exception_msg=True
):
    con = connector.connect(
        host=os.environ.get('MYSQL_HOST'),
        user=os.environ.get('MYSQL_USER'),
        password=os.environ.get('MYSQL_PASSWORD')
    )

    if is_dict:
        cursors = con.cursor(dictionary=True)
    else:
        cursors = con.cursor()

    try:
        if meny and meny_data:
            cursor = con.cursor()
            cursor.executemany(query, meny_data)
            con.commit()
            cursor.close()
            return True
        if data_value:
            cursors.execute(query, data_value)
        else:
            cursors.execute(query)

        if put:
            con.commit()
            return cursors.lastrowid
        elif get == 'one':
            return cursors.fetchone()
        else:
            return cursors.fetchall()
    except Exception as e:
        if exception_msg:
            print(f'Query did not executed due to :--> {e}')
    # finally:
    #     cursors.close()



def get_mysql_result(query):
    result = get_set_db_data(query)
    if not result:
        raise Exception(status_code=status.HTTP_204_NO_CONTENT, detail="No data found")
    return result

def get_set_db_data_with_session(query, get='', put='', meny='', meny_data='', is_dict=True):
    session = SessionLegecy()

    try:
        if meny and meny_data:
            session.executemany(query, meny_data)
            session.commit()
            session.close()
            return True
        else:
            result = session.execute(text(query))

        if put:
            lid = result.lastrowid
            session.commit()

            return lid
        elif get == 'one':
            column_names = result.keys()
            rows_as_dicts = dict(zip(column_names, result.fetchone()))
            return rows_as_dicts
        else:
            # Fetch all rows as dictionaries
            column_names = result.keys()
            rows_as_dicts = [
                dict(zip(column_names, row)) for row in result.fetchall()
            ]
            return rows_as_dicts
    except Exception as e:
        print(f'Query did not executed due to :--> {e}')
    # finally:
    #     session.close()


# def redis_con(db=''):
#     return redis.Redis(host='redis', db=db) if db else redis.Redis(host='redis')


def redis_con_gp(db):
    return redis.StrictRedis(
        host=os.environ.get('REDIS_HOST'),
        port=os.environ.get('REDIS_PORT'),
        db=db,
        password=os.environ.get('REDIS_PASSWORD')
    )

def send_email(subject, body, recipients_list, file_list=[]):
    # Set up the email parameters
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    sender_email = os.environ.get('EMAIL_USER')
    sender_password = os.environ.get('EMAIL_PASS')
    all_recipients = recipients_list
    to_email = ', '.join(all_recipients)

    # Create the MIME object
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = to_email
    message['Subject'] = subject

    # Attach the email body as plain text
    message.attach(MIMEText(body, 'html'))

    if file_list:
        for file in file_list:
            with open(file, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{file}"')
            message.attach(part)

    try:
        # Establish a connection to the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)

        # Identify yourself to the SMTP server
        server.starttls()

        # Log in to your Gmail account
        server.login(sender_email, sender_password)

        # Send the email
        server.sendmail(sender_email, all_recipients, message.as_string())

        # Close the connection
        server.quit()

        print('Email sent successfully!')
    except Exception as e:
        print(f'Email did not sent due to :--> {e}')


def get_sheet(wks_name, df, sheet_id='1bIVhsMMSCxeryXUJn7HRKHToZ950sUzJWV1ejD6puas'):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("sheet_key.json", scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)

    if not wks_name:
        return

    wks = sh.worksheet(wks_name)
    wks.append_rows(df.values.tolist())
