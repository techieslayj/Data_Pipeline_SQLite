#FIFO Work Queue
class workQueue:
	def __init__(self):
		self.queue = []

	def is_empty(self):
		return len(self.queue) == 0  # true if Empty

	def enQ(self, data):
		#debugging purposes
		# print(data)
		self.queue.append(data)

	def deQ(self):
		item = self.queue.pop(0) # removes and returns front of queue, makes sure data is uploaded in correct order to the database
		return item