import csv

adj_list = [ [] for _ in range(11922)] #11922
with open('routes.csv') as routes:
	reader = csv.reader(routes, delimiter=',', quotechar='"')
	for row in reader:
		u = int(row[0]) - 1
		v = int(row[1]) - 1
		adj_list[u].append(v)

for i in range(len(adj_list)):
	if(len(adj_list[i]) == 0):
		adj_list[i].append(-1)

source = 2912

# adj_list = [[1,3,4],[2,3],[3,4],[0,4],[-1]]
# source = 0

adj_list[source].append('$')

def map1(element):
	vertex = element[0]
	neighbours = element[1]
	output = []
	is_explored = False
	if '$' in neighbours:
		is_explored = True
	if is_explored == False:
		for n in neighbours:
			if(n != '$' and n!= source):
				output = output + [(vertex, n)]
	if is_explored == True:
		for n in neighbours:
			if n != '$' and n != -1 and n!= source:
				output = output + [(n,'$')]
	return output

def filterFunction(element):
	neighbours = element[1]
	# flag_number = False
	flag_dollar = False
	for i in neighbours:
		# if(i != '$'):
			# flag_number = True
		if(i == '$'):
			flag_dollar = True
	return flag_dollar

k = 10


rdd = sc.parallelize(zip(range(len(adj_list)),adj_list), 4)
for i in range(k+1):
	rdd = rdd.flatMap(map1)
	rdd = rdd.groupByKey()
	print 'Iteration: '+str(i+1)
	# print rdd.map(lambda x : (x[0], list(x[1]))).collect()
	# print rdd.filter(filterFunction).count()
	print rdd.filter(filterFunction).map(lambda x: x[0]).collect()
