import moth_tapi as m
import 

m.master_db_connect()

m.create_master_schema(m.master)

read_files = m.get_target_dbs()
for row in read_files:
	print(row)

m.create_master_schema(m.master)

m.temp_file_loop_func(m.read_files)

m.close_db()

print('End Of Line') 