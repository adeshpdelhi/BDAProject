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

V_n = 11922
source = 2912
destination = 2

# V_n = 6
# adj_list = [[1,3,4],[2,3],[3,4,5],[0,4],[-1],[-1]]
# source = 0
# destination = 5

adj_list[source].append('$')
parents = [ [] for _ in range(V_n)] 

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

def map2(element):
	vertex = element[0]
	neighbours = element[1]
	output = []
	is_explored = False
	if '$' in neighbours:
		is_explored = True
	if is_explored == False:
		for n in neighbours:
			if(n != '$' and isinstance(n,tuple)== False and n!= source):
				output = output + [(vertex, n)]
	if is_explored == True:
		for n in neighbours:
			if n != '$' and isinstance(n,tuple)== False and n != -1 and n!= source:
				output = output + [(n,'$'), (n,('@',vertex))]
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

def filterFunction2(element):
	neighbours = element[1]
	for n in neighbours:
		if isinstance(n, tuple):
			return True
	return False

def accumulate_new_edges(element):
	vertex = element[0]
	neighbours = element[1]
	for n in neighbours:
		if isinstance(n, tuple):
			return [(vertex, n[1])]

def map_keep_only_parent(element):
	vertex = element[0]
	parents = element[1]
	if parents[0] is None:
		return (vertex, parents[1])
	else:
		return (vertex, parents[0])

k = 20
part = 1

if part == 1:
	rdd = sc.parallelize(zip(range(len(adj_list)),adj_list), 4)
	visited_nodes = sc.parallelize([],4)
	for i in range(1,k+1):
		rdd = rdd.flatMap(map1)
		rdd = rdd.groupByKey()
		print 'Iteration: '+str(i)
		# print rdd.map(lambda x : (x[0], list(x[1]))).collect()
		# print rdd.filter(filterFunction).count()
		filteredRdd = rdd.filter(filterFunction).map(lambda x: x[0])
		print filteredRdd.collect()
		# print rdd.filter(filterFunction).map(lambda x: x[0]).collect()
		visited_nodes = visited_nodes.union(filteredRdd)
		if rdd.filter(filterFunction).count() == 0:
			print sorted(visited_nodes.distinct().collect())
			print "No more vertices found. Stopping!"
			break
		if i == k:
			print sorted(visited_nodes.collect())

if part == 2:
	rdd = sc.parallelize(zip(range(len(adj_list)),adj_list), 4)
	parentRdd = sc.parallelize(parents, 4)
	overallParentRdd = sc.parallelize([], 4)
	for i in range(1,k+1):
		rdd = rdd.flatMap(map2)
		rdd = rdd.groupByKey()
		print 'Iteration: '+str(i)
		# print rdd.map(lambda x : (x[0], list(x[1]))).collect()
		parentRdd = rdd.filter(filterFunction2)
		parentRdd = parentRdd.flatMap(accumulate_new_edges)
		overallParentRdd = overallParentRdd.fullOuterJoin(parentRdd)
		overallParentRdd = overallParentRdd.map(map_keep_only_parent)
		# print overallParentRdd.collect()
		if(overallParentRdd.lookup(destination)):
			overallParentRdd = overallParentRdd.sortByKey()
			current_vertex = destination
			print "Destination found"
			print str(destination) + " <-",
			while current_vertex != source:
				current_vertex = overallParentRdd.lookup(current_vertex)[0]
				print str(current_vertex) + " <-",
			break
		if rdd.filter(filterFunction).count() == 0:
			print "Destination not rechable!"
			break
		if i == k:
			print "Destination not found within k hops"


