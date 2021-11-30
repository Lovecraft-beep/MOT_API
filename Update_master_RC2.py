import moth_tapi_three as m

# Connect to Master
m.master_db_connect()

# Creates Schema
m.create_master_schema(m.master)

# Reads the Target Files
read_files = m.get_target_dbs()
for row in read_files:
	print(row)

# Copy the target DBs into the Temp Tables
m.process_file_loop_func(m.read_files)

# De Duplicate
m.de_dupe(m.master)

# Clean Up Generally
m.close_db()

# End in the Tradiitonal Way
print('End Of Line')
