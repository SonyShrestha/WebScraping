"""
    Author          :   Siddhi
    Created_date    :   2019/08/20  
    Modified Date   :   2019/09/26
    Description     :   Global variables    
"""
from sqlalchemy.orm import sessionmaker
import os
from configparser import ConfigParser


file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def initialize_config():
    try:
        global file_path
        global logging_level
        global log_to_console
        global driver_name
        global user
        global password
        global host
        global connection_url
        global temp_db_name
        global db_name
        global yeti_schedule_temp
        global buddha_schedule_temp
        global nac_schedule_temp
        global simrik_schedule_temp
        global sita_schedule_temp
        global tara_schedule_temp
        global yeti_status_temp
        global buddha_status_temp
        global tia_status_temp
        global schedule
        global status

        config = ConfigParser()
        config.read(os.path.abspath(file_path+'/config/config.ini'))
        logging_level = config.get('logger', 'level')
        log_to_console = int(config.get('logger', 'to_console'))

        driver_name = config.get('database', 'driver_name')
        user = config.get('database', 'user')
        password = config.get('database', 'password')
        host = config.get('database', 'host')

        temp_db_name = config.get('databases', 'temp_db_name')
        db_name = config.get('databases', 'db_name')

        yeti_schedule_temp = config.get('tables', 'yeti_schedule_temp')
        buddha_schedule_temp = config.get('tables', 'buddha_schedule_temp')
        nac_schedule_temp = config.get('tables', 'nac_schedule_temp')
        simrik_schedule_temp = config.get('tables', 'simrik_schedule_temp')
        sita_schedule_temp = config.get('tables', 'sita_schedule_temp')
        tara_schedule_temp = config.get('tables', 'tara_schedule_temp')
        yeti_status_temp = config.get('tables', 'yeti_status_temp')
        buddha_status_temp = config.get('tables', 'buddha_status_temp')
        tia_status_temp = config.get('tables', 'tia_status_temp')
        schedule = config.get('tables', 'schedule')
        status = config.get('tables', 'status')

        connection_url = f"{driver_name}://{user}:{password}@{host}/"


    except Exception as e:
        raise e


initialize_config()


def initialize_json_config():
    import json

    global bin_dict
    global score_map_dict
    global other_facts_dict
    global labels
    global quartile_division

    with open(os.path.abspath(file_path + '/config/config.json'), 'r') as js:
        js_conf = json.load(js)

    bin_dict = js_conf['bin']
    score_map_dict = js_conf['map_dict']
    other_facts_dict = js_conf['extra_facts']
    labels = js_conf['quartile_bin']['labels']
    quartile_division = js_conf['quartile_bin']['division']

#initialize_json_config()


def create_session():
    from sqlalchemy import create_engine
    global session
    global engine
    en_flag = 0
    ses_flag = 0
    try:
        engine = create_engine(connection_url)
        en_flag = 1
        Session = sessionmaker(bind=engine)
        session = Session()
        ses_flag = 1
    except Exception as e:
        if en_flag == 1:
            engine.dispose()
        if ses_flag == 1:
            session.close()
        raise e


create_session()
