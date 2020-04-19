#!/apps/python/install/bin/python
import configparser
import smtplib
import sys
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import cx_Oracle
import hvac
import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/current"
# os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib:$LD_LIBRARY_PATH"
# os.environ['PATH'] = "${ORACLE_HOME}/bin:$PATH"

def collect_property_file_contents(property_file, account_name=None):
    def as_dict(config):
        d = dict(config._sections)
        for k in d:
            d[k] = dict(config._defaults, **d[k])
            d[k].pop('__name__', None)
        return d

    try:
        config = configparser.ConfigParser()
        config.optionxform = str
        config.read(property_file)

        config_dict = as_dict(config)[account_name]

        return config_dict
    except Exception as e:
        print(
            'ERROR: Unable to open and collect property file contents for (property file: ' + property_file +
            ' account: ' + account_name + ')')
        print('ERROR: ' + str(e))
        exit(1)


def create_oracle_connection(db):
    """
    Create Oracle connection and return conn
    Calls open_oracle_connection

    :return: The oracle connection
    """

    try:
        property_file_contents = collect_property_file_contents('./config.ini', db)
        conn = open_oracle_connection(property_file_contents)
        return conn
    except Exception as e:
        print('ERROR: Unable to connect to ORACLE account. Will be unable to write output.')
        print('ERROR: ' + str(e))
        exit(1)


def open_oracle_connection(config_properties):
    """
    Establish a database connection with Oracle

    :param config_properties: This is the properties obtained from the utl pull of config.ini
    :return: The database connection object
    """
    # pdb.set_trace()
    try:
        dsn_tns = cx_Oracle.makedsn(config_properties['HOST'], config_properties['PORT'],
                                    service_name=config_properties['SERVICE_NAME'])
        conn = cx_Oracle.connect(user=config_properties['DB_USERNAME'],
                                 password=config_properties['DB_PASSWORD'], dsn=dsn_tns)

    except Exception as e:
        # print(f"Unable to connect to {config_properties['HOST']}@{config_properties['SERVICE_NAME']} due to {str(e).strip()}")
        print("ERROR %s" % e)
        sys.exit(1)

    return conn


def close_connection(conn):
    """
    Close a SF connection

    :param conn: this is the conn object when creating the connection
    """
    conn.commit()
    conn.close()


#
def execute_oracle_df_qry(conn, qry):
    """
    Execute given query on Oracle

    :param qry: This is the qry to be executed on Oracle
    :return: Given dataframe with output of requested query
    """
    cursor = conn.cursor()
    curOpen = cursor.execute(qry)
    oraCols = [row[0] for row in curOpen.description]
    df_oraData = pd.DataFrame(curOpen.fetchall(), columns=(oraCols))
    df_oraData.columns = [x.lower() for x in df_oraData.columns.tolist()]
    cursor.close()
    return df_oraData


def execute_oracle_qry(conn, qry):
    """
    Execute given query on Oracle without any return value

    :param conn: This is thevonnection to Oracle
    :param qry: This is the qry to be executed on Oracle
    """
    cursor = conn.cursor()
    curOpen = cursor.execute(qry)
    cursor.close()


def open_sf_connection(config_properties):
    """
    Establish a database connection. Assumes Snowflake.

    :param config_properties: This is the properties obtained from the util pull of config.ini
    :return: The database connection object
    """

    # Connect to Keeper to collect secrets
    client = hvac.Client(
        url=config_properties['KEEPER_URI'],
        namespace=config_properties['KEEPER_NAMESPACE'],
        token=config_properties['KEEPER_TOKEN'],
        verify=True
    )

    # Secrets are stored within the key entitled 'data'
    keeper_secrets = client.read(config_properties['KEEPER_SECRET_PATH'])['data']
    passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
    private_key = keeper_secrets['private_key']

    # PEM key must be byte encoded
    key = bytes(private_key, 'utf-8')
    # import pdb

    p_key = serialization.load_pem_private_key(
        key
        , password=passphrase.encode()
        , backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER
        , format=serialization.PrivateFormat.PKCS8
        , encryption_algorithm=serialization.NoEncryption())

    conn = snowflake.connector.connect(
        user=config_properties['CONNECTING_USER']
        , account=config_properties['ACCOUNT']
        , warehouse=config_properties['CONNECTING_WAREHOUSE']
        , role=config_properties['CONNECTING_ROLE']
        , private_key=pkb)

    return conn


def create_connection(host, port, service_name, user_name, password):
    try:
        dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
        conn = cx_Oracle.connect(user=user_name,
                                 password=password,
                                 dsn=dsn_tns)
    except Exception as e:
        sys.exit(1)
    return conn


def create_sf_connection(sf_account):
    """
    Create snowflake connection and return conn
    Calls open_sf_connection

    :param sf_account: An sf_account is the specific Snowflake environment being used
    :return: The snowflake connection
    """

    try:
        print("SF ACCOUNT %s" % sf_account)
        property_file_contents = collect_property_file_contents('./config.ini', sf_account.upper())
        conn = open_sf_connection(property_file_contents)
        return conn
    except Exception as e:
        print(sys.exc_info())
        print(f'ERROR: Unable to connect to {sf_account.upper()} account. Will be unable to write output.')
        print('ERROR: ' + str(e))
        exit(1)


def execute_sf_qry(conn, qry):
    """
    Execute given query on SF without any return value

    :param conn: This is thevonnection to SF
    :param qry: This is the qry to be executed on SF

    : return : this will return the response from SF
    """

    try:
        cur = conn.cursor()
        val = cur.execute(qry)
        sf_cols = [row[0] for row in val.description]
        df_sf_data = pd.DataFrame(val.fetchall(), columns=(sf_cols))
        df_sf_data.columns = [x.lower() for x in df_sf_data.columns.tolist()]
        return df_sf_data
    except Exception as e:
        pass


def send_email(tolist, subject, file_names):
    """
    Send an email to specific list of users

    : param tolist: list of emails to send the specific email too
    : param subject: subject of the email being sent
    : param msgbody : the message body of the email to be sent
    """

    msg = MIMEMultipart()
    # message = msgbody
    msgfrom = 'no-reply@cisco.com'
    msg['Subject'] = subject
    msg['From'] = msgfrom
    msg['To'] = tolist

    # msg.attach(MIMEText(text))

    file_list = file_names.split(',')
    for file in file_list:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename={}'.format(file))
        msg.attach(part)

    s = smtplib.SMTP('outbound.cisco.com')
    s.sendmail(msgfrom, tolist.split(","), msg.as_string())
    s.quit()
    return "Message sent!"
