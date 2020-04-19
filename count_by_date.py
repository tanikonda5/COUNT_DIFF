#!/apps/python/install/bin/python

import argparse
import os

import datacompy
import teradata

from utils import *

QUERIES = collect_property_file_contents('config.ini', 'QUERIES')
os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/current"
os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib:$LD_LIBRARY_PATH"
os.environ['PATH'] = "${ORACLE_HOME}/bin:$PATH"


def check_arg(args=None):
    parser = argparse.ArgumentParser(
        description="Script to validate metadata for each environment "
    )
    parser.add_argument('-t',
                        dest="table_name",
                        help='table_name',
                        required=True)
    parser.add_argument('-c',
                        dest="column_name",
                        help='column_name',
                        required=True)

    input_args = parser.parse_args(args)
    return input_args.table_name, input_args.column_name# , input_args.unique_key.upper()


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


def delta_count(table, column):
    oracle_conn = create_oracle_connection('EJC')
    records_df = execute_oracle_df_qry(oracle_conn, QUERIES['SELECT_QUERY'].format(table))

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

    ejc_conn = create_oracle_connection('EJC')
    db_type = execute_oracle_df_qry(ejc_conn, QUERIES['DB_TYPE'].format(job_stream))['source_db_type'][0]
    conn_name_df = execute_oracle_df_qry(ejc_conn,
                                         QUERIES['CONNECTION_NAME_QUERY'].format(source_db, source_schema))
    src_conn_name = conn_name_df.astype(str).values[0][0]

    if db_type == 'TERADATA':

        udaexec = teradata.UdaExec(appName="ModuleName", version="0.1", logConsole=False,
                                   odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",
                                   runNumberFile="/apps/edwsfdata/python/pylogs/.runNumber",
                                   logDir="/apps/edwsfdata/python/pylogs/logs")

        conn = udaexec.connect(method="odbc", DSN="tdprodetloffload")

    elif db_type == 'ORACLE':
        conn = get_src_connection('PRD', src_conn_name)
        # qry = QUERIES['ORACLE_UNIQUE_QUERY'].format(uk_str, source_schema, source_table, column, datetime, uk_str)
    qry = QUERIES['GROUP_BY_QUERY'].format(column, source_schema, source_table, column, column)
    import pdb
    #pdb.set_trace()

    df = execute_oracle_df_qry(conn, qry)

    sf_conn = create_sf_connection('SNOWFLAKE')

    sf_query = QUERIES['SF_GROUP_QUERY'].format(column, target_db, target_schema, target_table, column, column, column)

    sf_df = execute_sf_qry(sf_conn, sf_query)

    return df, sf_df


if __name__ == '__main__':
    table_name, column_name = check_arg(sys.argv[1:])
    src_df, dst_df = delta_count(table_name, column_name)

    src_df.to_excel("SRC_COUNTS_{}.xlsx".format(table_name))

    dst_df.to_excel("SF_COUNTS_{}.xlsx".format(table_name))

    send_email("{}@cisco.com".format(os.getlogin()), "Attention! Delta between two queries",
               "SRC_COUNTS_{}.xlsx,SF_COUNTS_{}.xlsx".format(table_name, table_name))
    # compare = datacompy.Compare(
    #     src_df,
    #     dst_df,
    # unique_df_metadata.to_excel("SF_{}.xlsx".format(table_name))
    # # unique_df_metadata.select_dtypes(include='float64').apply()

    #     df1_name='src_df',  # Optional, defaults to 'df1'
    #
    # unique_df_metadata = compare.df2_unq_rows
    #     df2_name='dst_df',
    #     join_columns={}.format(column_name))  # You can also specify a list of columns
    #
    # compare.matches(ignore_extra_columns=False)
    # unique_df_account = compare.df1_unq_rows
    # unique_df_account.to_excel("SRC_{}.xlsx".format(table_name))
