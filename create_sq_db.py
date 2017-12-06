import pandas as pd
from sqlalchemy import create_engine
import os, pdb, collections, multiprocessing, time
from functools import partial

def populate_db(file, path, database, chunksize = 100000):
	i = 0; j = 0
	print('Process {} file {}'.format(os.getpid(),file))
	for df in pd.read_csv(os.path.join(path+'//Data', r'{}'.format(file)), chunksize = chunksize, iterator = True):
		df = df.rename(columns={c:c.replace(' ', '_') for c in df.columns})
		df.index += j
		i += 1
		df.to_sql(file, database, if_exists = 'append')
		j = df.index[-1] + 1
	print('Done process {} file {}'.format(os.getpid(),file))
	return '{} completed.\n'.format(file)


def create_db(name,path, data_path, timeout = 15):
	database = create_engine('sqlite:///{}.db'.format(name), connect_args={'timeout' : timeout})
	list_files=[file for file in os.listdir(data_path) if not file.startswith('.')]

	start = time.time()

	pool=multiprocessing.Pool(processes=len(list_files))
	func = partial(populate_db, path = path)
	result = pool.map(func, list_files)

	end = time.time()
	pool.close()

	return '\nTime to completion: {}\n'.format(end - start)

def main_db(name, current_path, data_path):
	global database
	create_db(name, current_path, data_path)
	return 'Database {}.db created.\n'.format(name)
