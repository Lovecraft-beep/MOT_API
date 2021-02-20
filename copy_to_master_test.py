import moth_tapi as m

# Connect to Master
m.master_db_connect()

# Creates Schema If N0t Exist
m.create_master_schema(m.master)

read_files = m.get_target_dbs()
for row in read_files:
	print(row)

m.temp_file_loop_func(m.read_files)

# If the vehicle is not in Master - add it.
# m.create_vehicle_match(m.master, m.master_conn)

m.drop_temp_tables(m.master_conn)

m.close_db()

print('End Of Line')