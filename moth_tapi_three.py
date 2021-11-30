# Where all the MOT Database Functions Live
# A more elegant approach
# For a more civilised age

# Imports
import sqlite3
import glob, os
import time
from datetime import datetime

# Optimise Pragma
def optimize_pragma():
    start_time = time.time()
    print('Optimizing Database')
    master.execute('''PRAGMA optimize''')
    print('Completed in :','%s seconds' % (time.time() - start_time))
    
# Creates / connects to the master database
def master_db_connect():
    fin_db_name = ('full_extract_mac' + '.db')
    global master, master_conn
    master_conn = sqlite3.connect(fin_db_name, isolation_level='DEFERRED')
    master = master_conn.cursor()
    master.execute('''PRAGMA synchronous = OFF''')
    master.execute('''PRAGMA journal_mode = OFF''')
    master.execute('''PRAGMA auto_vacuum = 1''')
    return master, master_conn

# Grabs the target dbs from the DB_Target database
def get_target_dbs():
    os.chdir('/home/fraser/Documents')
    global read_files
    t_read_files = glob.glob('*.db')
    read_files = sorted(t_read_files)
    return read_files


# Creates the schema for the master db
def create_master_schema(master):
    master.execute('''CREATE TABLE IF NOT EXISTS vehicles
    (VID INTEGER,
    vehicleid TEXT,
    vrm TEXT,
    valid TEXT,
    make TEXT,
    model TEXT,
    colour TEXT,
    fuel TEXT,
    enginesize TEXT,
    dateregistered TEXT)''')

    master.execute('''CREATE TABLE IF NOT EXISTS mottests
    (TID INTEGER,
    testnumber TEXT,
    result TEXT,
    date TEXT,
    mileage TEXT,
    expiry TEXT,
    TVID TEXT)''')

    master.execute('''CREATE TABLE IF NOT EXISTS rfrs
    (RID INTEGER,
    advisory TEXT,
    type TEXT,
    danger TEXT(4),
    RTID TEXT)''')

# This is the main loop for populating tables in master
def process_file_loop_func(read_files):
    for f in read_files:
        print('Reading :',f)
        # Connect to target DB
        slave_db_cn(f)
        #  Go to Subroutine to populate tables
        trigger_get_slave(slave)

# Connect to the target db
# & get the contents   
def slave_db_cn(f):
    global slave, slave_conn
    slave_conn = sqlite3.connect(f, isolation_level='DEFERRED')
    slave = slave_conn.cursor()
    slave.execute('''PRAGMA synchronous = OFF''')
    slave.execute('''PRAGMA journal_mode = OFF''')
    slave.execute('''PRAGMA auto_vacuum = 1''')
    slave_conn.commit()
    return slave, slave_conn

# Sub for populating Temp Tables from target DB
def trigger_get_slave(slave): 
    slave_get_vehicles(slave)
    slave_get_tests(slave)
    slave_get_rfrs(slave)

# sub for selecting all the vehicles from target
def slave_get_vehicles(slave):
    vt_time = datetime.now()
    slave.execute('''select * from vehicles''')
    for row in slave:
        slave_vehicle_output = row
        populate_master_vehicles(slave_vehicle_output)
    print('Completed in :','%s seconds' % (datetime.now() - vt_time))

# sub for selecting all the tests from target
def slave_get_tests(slave):
    tt_time = datetime.now()
    slave.execute('''select * from mottests''')
    for row in slave:
        slave_test_output = row
        populate_master_tests(slave_test_output)
    print('Completed in :','%s seconds' % (datetime.now() - tt_time))

# sub for selecting all the rfrs from target
def slave_get_rfrs(slave):
    rt_time = datetime.now()
    slave.execute('''select * from rfrs''')
    for row in slave:
        slave_rfr_output = row
        populate_master_rfrs(slave_rfr_output)
    print('Completed in :','%s seconds' % (datetime.now() - rt_time))

# sub for populating slave vehicle table
def populate_master_vehicles(slave_vehicle_output):

    vid = (slave_vehicle_output[1])
    reg = (slave_vehicle_output[2])
    valid = (slave_vehicle_output[3])
    make = (slave_vehicle_output[4])
    model = (slave_vehicle_output[5])
    colour = (slave_vehicle_output[6])
    fuel = (slave_vehicle_output[7])
    engsize = (slave_vehicle_output[8])
    registered = (slave_vehicle_output[9])

    stream = [(vid, reg, valid, make, model, colour, fuel, engsize, registered)]
    param = """INSERT INTO vehicles (vehicleid,
    vrm,
    valid,
    make,
    model,
    colour,
    fuel,
    enginesize,
    dateregistered) 
    VALUES (?,?,?,?,?,?,?,?,?);"""
    
    main_db_commit(stream, param)
    

# sub for populating slave test table
def populate_master_tests(slave_test_output):

    testNumber = (slave_test_output[1])
    result = (slave_test_output[2])
    testDate = (slave_test_output[3]) 
    odovalue = (slave_test_output[4])
    expiry = (slave_test_output[5])
    vuid = (slave_test_output[6])
    
    stream = [(testNumber, result, testDate, odovalue, expiry, vuid)]

    param = """INSERT INTO mottests (
    testnumber,
    result,
    date,
    mileage,
    expiry,
    TVID) 
    VALUES (?,?,?,?,?,?);"""

    main_db_commit(stream,param)
    

# sub for populating slave rfr table
def populate_master_rfrs(slave_rfr_output):

    text = (slave_rfr_output[1])
    ftype = (slave_rfr_output[2])
    dang = (slave_rfr_output[3])
    rtid = (slave_rfr_output[4])

    stream = [(text, ftype, dang, rtid)]

    param = """INSERT INTO rfrs (
    advisory,
    type,
    danger,
    RTID) 
    VALUES (?,?,?,?);"""

    main_db_commit(stream, param)
    

# De-Dupe-Temp
def de_dupe(master):
  
    # De Dupe Vehicles
    start_time = time.time()
    print('De-Dupe Vehicles Table')
    master.execute('''DELETE FROM vehicles
      WHERE rowid NOT IN (
    SELECT min(rowid) 
      FROM vehicles
     GROUP BY vehicleid,
              vrm,
              valid,
              make,
              model,
              colour,
              fuel,
              enginesize,
              dateregistered
)''')
    master_conn.commit()
    print('De-Dupe Vehicles table completed in :','%s seconds' % (time.time() - start_time))

    # De Dupe MOTTESTs
    start_time = time.time()
    print('De-Dupe MOT Tests Table')
    master.execute('''DELETE FROM mottests
      WHERE rowid NOT IN (
    SELECT min(rowid) 
      FROM mottests
     GROUP BY testnumber,
              TVID
)''')
    master_conn.commit()
    print('De-Dupe MOT Test table completed in :','%s seconds' % (time.time() - start_time))

  # De Dupe RFRS
    start_time = time.time()
    print('De-Dupe RFRS Table')
    master.execute('''DELETE FROM rfrs
      WHERE rowid NOT IN (
    SELECT min(rowid) 
      FROM rfrs
     GROUP BY advisory, RTID
)''')
    master_conn.commit()
    print('De-Dupe RFRS table completed in :','%s seconds' % (time.time() - start_time))
    
# Commit master sub
def main_db_commit(stream, param):
    master.executemany(param, stream)
    master_conn.commit()

# close the dbs
def close_db():
    master.close
