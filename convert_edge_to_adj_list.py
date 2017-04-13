import csv
import pickle

adj_list = [ [] for _ in range(11922)] #11922
with open('routes.csv') as routes:
	reader = csv.reader(routes, delimiter=',', quotechar='"')
	for row in reader:
		u = int(row[0]) - 1
		v = int(row[1]) - 1
		adj_list[u].append(v)

with open("adj_list","wb") as out_file:
	pickle.dump(adj_list, out_file)


max_value = -1
max_i = -1
for i in range(len(adj_list)):
	if(len(adj_list[i])>max_value):
		max_value = max(max_value, len(adj_list[i]))
		max_i = i


# import pickle

# with open("adj_list","rb") as in_file:
# 	adj_list = pickle.load(in_file)

# import csv
# import pickle

# adj_list = [ [] for _ in range(11922)] #11922

# with open('routes.csv') as routes:
# 	reader = csv.reader(routes, delimiter=',', quotechar='"')
# 	for row in reader:
# 		u = int(row[0]) - 1
# 		v = int(row[1]) - 1
# 		adj_list[u].append(v)

# adj_list[2931].append('$')
# adj_list[3681].append('$')