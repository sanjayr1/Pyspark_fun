#Sanjay Roberts
from p1_class import Points

from pyspark import SparkContext, SparkConf
import math
import sys
from operator import add

print(sys.argv)
cellSize = int(sys.argv[1])
input_file = sys.argv[2]
output_dir = sys.argv[3]
max_dist = float(sys.argv[4])

conf = SparkConf().setAppName("Grid Points")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

file = sc.textFile(input_file)

def fix_list(x):
    x = x.split(",")
    return Points(x[0], float(x[1]), float(x[2]))

points = file.map(fix_list)
cell_location = points.map(lambda point: point.find_cells(cellSize))
cell_location_flat = cell_location.flatMap(lambda x: [k for k in x])

comb = cell_location_flat.combineByKey(
        lambda row: [row],
        lambda rows, row: rows + [row],
        lambda rows1, rows2: rows1 + rows2,
    )

sorted_cells = comb.sortByKey()

def distance(p0, p1):
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)

from itertools import combinations

res = sorted_cells.filter(lambda x: len(x[1])>1)

def combo(x):
     co = combinations(x[1], 2)
     new = []
     for c in co:
          p1 = Points(c[0][0], c[0][1], c[0][2])
          p2 = Points(c[1][0], c[1][1], c[1][2])
          point1 = (p1.X_val, p1.Y_val)
          point2 = (p2.X_val, p2.Y_val)
          dist = distance(point1, point2)
          if dist <= max_dist:
               new.append((p1,p2))
     return new

res_list = res.map(combo).flatMap(lambda x: x)
res_list1 = res_list.map(lambda t: tuple(sorted(t, key= lambda obj: obj.name))).persist()
fixed_res = res_list1.map(lambda a: (a[0].name, a[1].name))
sorted_res = fixed_res.map(lambda t: tuple(sorted(t)))
final = sorted_res.distinct()
final.saveAsTextFile(output_dir)
my_dist = res_list1.map(lambda a: distance((a[0].X_val, a[0].Y_val),(a[1].X_val, a[1].Y_val))).persist()
my_dist = my_dist.distinct().persist()

max_distance = my_dist.reduce(max)
min_distance = my_dist.reduce(min)
avg_distance = my_dist.reduce(add)/my_dist.count()

print('Average distance = {}'.format(max_distance))
print('Min distance = {}'.format(min_distance))
print('Max distance = {}'.format(avg_distance))

sc.stop()



