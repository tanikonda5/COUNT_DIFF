#!/apps/python/install/bin/python

import argparse
import os

import datacompy
import teradata

from utils import *

QUERIES = collect_property_file_contents('config.ini', 'QUERIES')
os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/current"
os.environ[
    'LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib:$LD_LIBRARY_PATH"
os.environ['PATH'] = "${ORACLE_HOME}/bin:$PATH"


def check_arg(args=None):
    parser = argparse.ArgumentParser(
        description="Script to validate metadata for each environment "
    )
    parser.add_argument('-t',
                        dest="table_name",
                        help='table_name',
                        required=True)
    parser.add_argument('-db',
                        dest="db_type",
                        help='db_type',
                        required=True)
    parser.add_argument('-c',
                        dest="column_name",
                        help='column_name',
                        required=True)
    parser.add_argument('-d',
                        dest="date",
                        help='date',
                        required=True)

    input_args = parser.parse_args(args)
    return input_args.table_name, input_args.db_type.upper(), input_args.column_name, input_args.date.upper()


def get_src_connection(environment_name, connection_name):
    eds_conn = create_oracle_connection('EDS')
    param_df = execute_oracle_df_qry(eds_conn, QUERIES['PARAM_QUERY'].format(environment_name, connection_name))

    if len(param_df) == 0:
        print(
            "Could not find connections details for connection name - " + connection_name + " in Repository database. Hence cannot proceed with metadata collection")
        sys.exit(0)

    param_dict = {row[0]: row[1] for row in param_df.values}
    param_dict['SOURCE_LOGIN_PASSWORD'] = execute_oracle_df_qry(eds_conn, QUERIES['PASSWORD_QUERY']
                                                                .format(param_dict['SOURCE_LOGIN_PASSWORD'])).values[0][
        0]

    close_connection(eds_conn)

    source_data_connection = create_connection(param_dict['SOURCE_HOST'], param_dict['SOURCE_PORT'],
                                               param_dict['SOURCE_SERVICE_NAME'], param_dict['SOURCE_LOGIN'],
                                               param_dict['SOURCE_LOGIN_PASSWORD'])

    return source_data_connection


def delta_count(table, src_type, column, datetime):
    oracle_conn = create_oracle_connection('EJC')
    records_df = execute_oracle_df_qry(oracle_conn, QUERIES['SELECT_QUERY'].format(table))
    import pdb
    #pdb.set_trace()
    if records_df.empty:
        raise ValueError('\n\nJCT_QUERY: {}\n\n'.format(
            QUERIES['SELECT_QUERY'].format(table)) + 'The above query is returning empty records\n\n')

    else:

        if records_df['target_schema'][0] == 'STG':
            source_db = records_df['source_db_name'][0]
            source_schema = records_df['source_schema'][0]
            source_table = records_df['source_table_name'][0]
            target_db = records_df['target_db_name'][1]
            target_schema = records_df['target_schema'][1]
            target_table = records_df['target_table_name'][0]
            job_stream = records_df['job_stream_id'][0]

        elif records_df['target_schema'][1] == 'STG':
            source_db = records_df['source_db_name'][1]
            source_schema = records_df['source_schema'][1]
            source_table = records_df['source_table_name'][1]
            target_db = records_df['target_db_name'][0]
            target_schema = records_df['target_schema'][0]
            target_table = records_df['target_table_name'][1]
            job_stream = records_df['job_stream_id'][1]

    eds_conn = create_oracle_connection('EDS')
    uk_df = execute_oracle_df_qry(eds_conn, QUERIES['CATALOG_QUERY'].format(source_table, source_schema))
    uk_df.values.tolist()

    if uk_df.empty:
        sys.exit(1)
    else:
        uk_list = uk_df['column_name'].values.tolist()
        uk_str = ",".join(uk_list)

    ejc_conn = create_oracle_connection('EJC')
    # db_df = execute_oracle_df_qry(ejc_conn, QUERIES['DB_TYPE'].format(job_stream))
    # if db_df.empty:
    #     raise ValueError('\n\nDB_TYPE_QUERY: {}\n\n'.format(QUERIES['DB_TYPE'].format(job_stream)) + 'The above query is returning empty records\n\n')
    #     sys.exit(1)
    # db_type = db_df['source_db_type'][0]
    conn_name_df = execute_oracle_df_qry(ejc_conn,
                                         QUERIES['CONNECTION_NAME_QUERY'].format(source_db, source_schema))
    src_conn_name = conn_name_df.astype(str).values[0][0]
    #pdb.set_trace()
    if src_type == 'T':

        udaexec = teradata.UdaExec(appName="ModuleName", version="0.1", logConsole=False,
                                   odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",
                                   runNumberFile="/apps/edwsfdata/python/pylogs/.runNumber",
                                   logDir="/apps/edwsfdata/python/pylogs/logs")

        #conn = udaexec.connect(method="odbc", DSN="tdprodetloffload")
        conn = udaexec.connect(method="odbc", DSN="tdprodetloffload", charset='UTF8')
        qry = QUERIES['TD_UNIQUE_QUERY'].format(uk_str, source_schema, source_table, column, datetime, uk_str)

    elif src_type == 'O':
        conn = get_src_connection('PRD', src_conn_name)
        qry = QUERIES['ORACLE_UNIQUE_QUERY'].format(uk_str, source_schema, source_table, column, datetime, uk_str)

    else:
        print("\nPlease provide right 'DB_TYPE' it should either 'T' OR 'O'\n")
        sys.exit(1)

    print("\nSource Query:\n{query}\n".format(query=qry))
    df = execute_oracle_df_qry(conn, qry)
    close_connection(conn)

    source_df = pd.DataFrame()
    source_df['{}'.format(uk_str)] = df[df.columns.tolist()].apply(lambda row: ','.join(row.values.astype(str)), axis=1)
    source_df.replace(to_replace="\.0+", value="", regex=True, inplace=True)
    source_df['{}'.format(uk_str)] = source_df['{}'.format(uk_str)].str.strip()

    sf_conn = create_sf_connection('SNOWFLAKE')
    sf_qry = QUERIES['SF_QUERY'].format(uk_str, target_db, target_schema, target_table, column, datetime, uk_str)
    print("\nSF_QUERY:\n{sf_query}\n".format(sf_query=sf_qry))
    sf_df = execute_sf_qry(sf_conn, sf_qry)
    close_connection(sf_conn)

    dest_df = pd.DataFrame()
    dest_df['{}'.format(uk_str)] = sf_df[sf_df.columns.tolist()].apply(lambda row: ','.join(row.values.astype(str)),
                                                                       axis=1)
    dest_df.replace(to_replace="\.0+", value="", regex=True, inplace=True)
    dest_df['{}'.format(uk_str)] = dest_df['{}'.format(uk_str)].str.strip()

    return source_df, dest_df, uk_list


if __name__ == '__main__':
    table_name, db_type, column_name, date = check_arg(sys.argv[1:])
    src_df, dst_df, unique_keys = delta_count(table_name, db_type, column_name, date)

    compare = datacompy.Compare(
        src_df,
        dst_df,
        df1_name='src_df',  # Optional, defaults to 'df1'
        df2_name='dst_df',
        join_columns='{}'.format(",".join(unique_keys)))  # You can also specify a list of columns

    compare.matches(ignore_extra_columns=False)
    unique_df_account = compare.df1_unq_rows
    unique_df_account.to_excel("SRC_{}.xlsx".format(table_name), index=False)

    unique_df_metadata = compare.df2_unq_rows
    unique_df_metadata.to_excel("SF_{}.xlsx".format(table_name), index=False)

    send_email("{}@cisco.com".format(os.getlogin()), "Attention! Delta between two queries",
               "SRC_{}.xlsx,SF_{}.xlsx".format(table_name, table_name))

