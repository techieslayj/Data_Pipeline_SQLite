import sqlite3 as sql

# Loading things into appropiate tables using dataworker class

class DataWorker:
	def __init__(self, queue):
		self.linkedQ = queue
		#File path for the SQLite DB 
		self.sqlLiteFilePath = 'INsert DATABASE FILE HERE'

	def load_data(self):

		print("SQLLite Data Loading Service Started")
		while not self.linkedQ.is_empty():  # If the work queue is not empty then continue unloading data until it is
			
			#Data from the parser file and ensures work queue is offloading the data correctly	
			record = self.linkedQ.deQ()

			#Ensure that the records are going into appropiate table
			if record['dest'] == 'agent':
				#debugging purposes
				#print(record.keys())
				#print(record.values())
				
				#ESTABLISH SQLLITE DB CONNECTION AND ADD DATA
				connection = sql.connect(self.sqlLiteFilePath)
				cur = connection.cursor()
				query = 'INSERT INTO agents VALUES (NULL, :name, :agent_code, :phone, :city, :state, :zip)'
				cur.execute(query, record)
				##Debugging purposes
				#print(cur.lastrowid)
				connection.commit()
				connection.close()
		

			if record['dest'] == 'offices':
				#Debugging purposes
				#print(record.keys())
				#print(record.values())
				
				#ESTABLISH SQLLITE DB CONNECTION AND ADD DATA
				connection = sql.connect(self.sqlLiteFilePath)
				cur = connection.cursor()
				query2 = 'INSERT INTO offices VALUES (NULL, :name, :office_code, :phone, :city, :state, :zip)'
				cur.execute(query2, record)
				# #Debugging purposes
				# print(cur.lastrowid)
				connection.commit()
				connection.close()

			if record['dest'] == 'listings':
				#Debugging purposes
				#print(record.keys())
				#print(record.values())

				#ESTABLISH SQLLITE DB CONNECTION AND ADD DATA
				connection = sql.connect(self.sqlLiteFilePath)
				cur = connection.cursor()
				query3 = 'INSERT INTO listings VALUES (NULL, :address, :city, :state, :zip, :mls_number, :price, :status, :type, :description, :agent_id, :office_id)'
				cur.execute(query3, record)
				# #Debugging purposes
				# print(cur.lastrowid)
				connection.commit()
				connection.close()
		
		print("Load Data Service Successful")                # let's us know success of adding all data to table(s)