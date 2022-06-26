import mysql.connector
from mysql.connector import errorcode
import logging
from sorcery import dict_of


logger = logging.getLogger(__name__)


def create_connection():
    conn = None
    try:
        conn = mysql.connector.connect(user='root', password='bijeck',
                              host='localhost',
                              database='SAFETY',
                              autocommit=True)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(err)
    return conn

def create_table():
    conn = create_connection()
    response_code = 200
    if conn is None:
        raise ConnectionError("Cant create connection")
    try:
        cursor = conn.cursor()
        create_table_sql = """CREATE TABLE IF NOT EXISTS SAFETY(
                                                                Id INT AUTO_INCREMENT,
                                                                Date VARCHAR(255) NOT NULL,
                                                                InjuryLocation VARCHAR(255) NOT NULL,
                                                                Gender VARCHAR(255) NOT NULL,
                                                                AgeGroup VARCHAR(255) NOT NULL,
                                                                IncidentType VARCHAR(255) NOT NULL,
                                                                DaysLost VARCHAR(255) NOT NULL,
                                                                Plant VARCHAR(255) NOT NULL,
                                                                ReportType VARCHAR(255) NOT NULL,
                                                                Shift VARCHAR(255) NOT NULL,
                                                                Department VARCHAR(255) NOT NULL,
                                                                IncidentCost VARCHAR(255) NOT NULL,
                                                                WkDay VARCHAR(255) NOT NULL,
                                                                Month VARCHAR(255) NOT NULL,
                                                                Year VARCHAR(255) NOT NULL,
                                                                PRIMARY KEY (Id));
                                TRUNCATE TABLE SAFETY;
                            """
        cursor.execute(create_table_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_CANT_CREATE_TABLE:
            logger.error("Cant not create tables")
            logger.error(err.msg)
        response_code =  err.errno
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close() 
    return response_code

def insert_to_table(list_of_dict: list):
    conn = create_connection()
    if conn is None:
        raise ConnectionError("Cant create connection")

    if len(list_of_dict) == 0:
        logger.error("No record is ready to be inserted")
        raise ValueError("No record is ready to be inserted.")

    response_code = 200
    try:
        cursor = conn.cursor()
        insert_to_table_sql = """INSERT INTO SAFETY (Date,
                                                    InjuryLocation,
                                                    Gender,
                                                    AgeGroup,
                                                    IncidentType,
                                                    Days,
                                                    Plant,
                                                    ReportType,
                                                    Shift,
                                                    Department,
                                                    IncidentCost,
                                                    WkDay,
                                                    Month,
                                                    Year)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,);
                                """
        for record in list_of_dict:
            values = (record['Date'],
                        record['InjuryLocation'],
                        record['Gender'],
                        record['AgeGroup'],
                        record['IncidentType'],
                        record['Days'],
                        record['Plant'],
                        record['ReportType'],
                        record['Shift'],
                        record['Department'],
                        record['IncidentCost'],
                        record['WkDay'],
                        record['Month'],
                        record['Year'])
            print(values)
            cursor.execute
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_INSERT_INFO:
            logger.error("Cant not insert record to table")
            logger.error(err.msg)
        response_code =  err.errno
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close() 
    return response_code

code = create_table()
print(code)
list_value = ["1-Jan-20","Multiple","Male","25-34","Burn","0.0","Iowa","Near Miss","Afternoon","Painting","$0" ,"Wed","1","2020"]
Date,InjuryLocation,Gender,AgeGroup,IncidentType,Days,Plant,ReportType,Shift,Department,IncidentCost,WkDay,Month,Year = list_value

dict_value = dict_of(Date,InjuryLocation,Gender,AgeGroup,IncidentType,Days,Plant,ReportType,Shift,Department,IncidentCost,WkDay,Month,Year)
data = [dict_value]
code = insert_to_table(data)
print(code)