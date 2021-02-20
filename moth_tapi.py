# Where all the MOT Database Functions Live
# A more elegant approach
# For a more civilised age

# Imports
import sqlite3
import glob, os



# Creates / connects to the master database
def master_db_connect():
    fin_db_name = ('master_test' + '.db')
    global master, master_conn
    master_conn = sqlite3.connect(fin_db_name, isolation_level='DEFERRED')
    master = master_conn.cursor()
    master.execute('''PRAGMA synchronous = OFF''')
    master.execute('''PRAGMA journal_mode = OFF''')
    return master, master_conn

# Grabs the target dbs from the DB_Target database
def get_target_dbs():
    os.chdir('/Users/fraserlevey/Downloads/PyFiles/mot3-merge/DB_Targets')
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

    master.execute('''CREATE TABLE IF NOT EXISTS old_vrm
    (VRMID INTEGER,
    vrm TEXT,
    VVID INTEGER)''')

# Temp dbs to handle db updates from imports
    master.execute('''CREATE TABLE IF NOT EXISTS temp_vehicles
    (temp_VID INTEGER,
    temp_vehicleid TEXT,
    temp_vrm TEXT,
    temp_valid TEXT,
    temp_make TEXT,
    temp_model TEXT,
    temp_colour TEXT,
    temp_fuel TEXT,
    temp_enginesize TEXT,
    temp_dateregistered TEXT)''')

    master.execute('''CREATE TABLE IF NOT EXISTS temp_mottests
    (temp_TID INTEGER,
    temp_testnumber TEXT,
    temp_result TEXT,
    temp_date TEXT,
    temp_mileage TEXT,
    temp_expiry TEXT,
    temp_TVID TEXT)''')

    master.execute('''CREATE TABLE IF NOT EXISTS temp_rfrs
    (temp_RID INTEGER,
    temp_advisory TEXT,
    temp_type TEXT,
    temp_danger TEXT(4),
    temp_RTID TEXT)''')

# This is the main loop for populating the temp tables in master
def temp_file_loop_func(read_files):
    for f in read_files:
        print('Reading :',f)
        slave_db_cn(f)
        trigger_get_slave(slave)
        create_vehicle_match(master, master_conn)     
        drop_temp_tables(master)  
        create_master_schema(master)

# Connect to the target db
# & get the contents   
def slave_db_cn(f):
    global slave, slave_conn
    slave_conn = sqlite3.connect(f, isolation_level='DEFERRED')
    slave = slave_conn.cursor()
    slave.execute('''PRAGMA synchronous = OFF''')
    slave.execute('''PRAGMA journal_mode = OFF''')
    slave_conn.commit()
    return slave, slave_conn

def trigger_get_slave(slave): 
    slave_get_vehicles(slave)
    slave_get_tests(slave)
    slave_get_rfrs(slave)

# sub for selecting all the vehicles from target
def slave_get_vehicles(slave):
    slave.execute('''select * from vehicles''')
    slave_vehicle_output = slave.fetchall()
    populate_temp_vehicles(slave_vehicle_output)

# sub for selecting all the tests from target
def slave_get_tests(slave):
    slave.execute('''select * from mottests''')
    slave_test_output = slave.fetchall()
    populate_temp_tests(slave_test_output)

# sub for selecting all the rfrs from target
def slave_get_rfrs(slave):
    slave.execute('''select * from rfrs''')
    slave_rfr_output = slave.fetchall()
    populate_temp_rfrs(slave_rfr_output)

# sub for populating master vehicle table
def populate_master_vehicles(master_vehicle_output):
    print('Total Vehicle Rows :', len(master_vehicle_output))
    for row in master_vehicle_output:
        vid = (row[1])
        reg = (row[2])
        valid = (row[3])
        make =(row[4])
        model = (row[5])
        colour = (row[6])
        fuel = (row[7])
        engsize = (row[8])
        registered = (row[9])

        stream = [(vid, reg, valid, make, model, colour, fuel, engsize, registered)]
        param = """INSERT INTO vehicles (VID, vehicleid,
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

# sub for populating slave vehicle table
def populate_temp_vehicles(slave_vehicle_output):
    print('Populate test vehicles')
    for row in slave_vehicle_output:
        vid = (row[1])
        reg = (row[2])
        valid = (row[3])
        make =(row[4])
        model = (row[5])
        colour = (row[6])
        fuel = (row[7])
        engsize = (row[8])
        registered = (row[9])

        stream = [(vid, reg, valid, make, model, colour, fuel, engsize, registered)]
        param = """INSERT INTO temp_vehicles (temp_vehicleid,
        temp_vrm,
        temp_valid,
        temp_make,
        temp_model,
        temp_colour,
        temp_fuel,
        temp_enginesize,
        temp_dateregistered) 
        VALUES (?,?,?,?,?,?,?,?,?);"""
        
        main_db_commit(stream, param)

# sub for populating master test table
def populate_master_tests(master_test_output):
    print('Total MOT Test Rows :', len(master_test_output))
    for row in master_test_output:
        testNumber = (row[1])
        result = (row[2])
        testDate = (row[3]) 
        odovalue = (row[4])
        expiry = (row[5])
        vuid = (row[6])
        
        stream = [(testNumber, result, testDate, odovalue, expiry, vuid)]

        param = """INSERT INTO mottests (TID, 
        testnumber,
        result,
        date,
        mileage,
        expiry,
        TVID) 
        VALUES (?,?,?,?,?,?);"""

        main_db_commit(stream,param)

# sub for populating slave test table
def populate_temp_tests(slave_test_output):
    print('Populate temp tests')
    for row in slave_test_output:
        testNumber = (row[1])
        result = (row[2])
        testDate = (row[3]) 
        odovalue = (row[4])
        expiry = (row[5])
        vuid = (row[6])
        
        stream = [(testNumber, result, testDate, odovalue, expiry, vuid)]

        param = """INSERT INTO temp_mottests (
        temp_testnumber,
        temp_result,
        temp_date,
        temp_mileage,
        temp_expiry,
        temp_TVID) 
        VALUES (?,?,?,?,?,?);"""

        main_db_commit(stream,param)

# sub for populating master rfr table
def populate_master_rfrs(master_rfr_output):
    print('Total RFR Rows :', len(master_rfr_output))
    for row in master_rfr_output:
        text = (row[1])
        ftype = (row[2])
        dang = (row[3])
        rtid = (row[4])

        stream = [(text, ftype, dang, rtid)]

        param = """INSERT INTO rfrs (
        RFID,
        advisory,
        type,
        danger,
        RTID) 
        VALUES (?,?,?,?);"""

        main_db_commit(stream, param)

# sub for populating slave rfr table
def populate_temp_rfrs(slave_rfr_output):
    print('Populate temp rfrs')
    for row in slave_rfr_output:
        text = (row[1])
        ftype = (row[2])
        dang = (row[3])
        rtid = (row[4])

        stream = [(text, ftype, dang, rtid)]

        param = """INSERT INTO temp_rfrs (
        temp_advisory,
        temp_type,
        temp_danger,
        temp_RTID) 
        VALUES (?,?,?,?);"""

        main_db_commit(stream, param)

def get_vid_master(master):
    master.execute('''SELECT VID, vehicleid FROM vehicles''')
    vid_output = master.fetchall()
    return vid_output

def get_vid_temp(master):
    master.execute('''SELECT temp_VID, temp_vehicleid FROM temp_vehicles''')
    temp_vid_output = master.fetchall()
    return temp_vid_output

def create_vehicle_match(master, master_conn):
    print('Vehicle SQL')
    master.execute('''INSERT INTO vehicles(VID, vehicleid, vrm, valid, make, model, colour, fuel, enginesize, dateregistered)
    SELECT * FROM temp_vehicles v1
    WHERE NOT EXISTS(
        SELECT * 
    FROM vehicles v2
        WHERE v1.temp_vehicleid = v2.vehicleid
    );''')
    master_conn.commit()

    print('Test SQL')
    master.execute('''INSERT INTO mottests (TID, testnumber, result, date, mileage, expiry, TVID)
    SELECT * FROM temp_mottests m1
    WHERE NOT EXISTS(
        SELECT * 
    FROM mottests m2
        WHERE m1.temp_testnumber = m2.testnumber
    ); ''')
    master_conn.commit()

    print('RFR SQL')
    master.execute('''INSERT INTO rfrs (RID, advisory, type, danger, RTID)
    SELECT * FROM temp_rfrs r1
    WHERE NOT EXISTS(
        SELECT * 
    FROM rfrs r2
        WHERE r1.temp_RTID = r2.RTID
    AND
        r1.temp_advisory = r2.advisory
    );''')
    master_conn.commit()

def refresh_master_vehicle(match_vehicle):
    for row in match_vehicle:
        vid = (row[1])
        reg = (row[2])
        valid = (row[3])
        make =(row[4])
        model = (row[5])
        colour = (row[6])
        fuel = (row[7])
        engsize = (row[8])
        registered = (row[9])

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

        print('VehicleID:',vid, ' VRM:', reg, ' Commited to Master')
        
        main_db_commit(stream, param)


# Commit master sub
def main_db_commit(stream, param):
    master.executemany(param, stream)
    master_conn.commit()

# Drop temp tables
def drop_temp_tables(master):
    master.execute('''DROP TABLE temp_vehicles;''')
    master.execute('''DROP TABLE temp_mottests;''')
    master.execute('''DROP TABLE temp_rfrs;''')

# close the dbs
def close_db():
    master.close


