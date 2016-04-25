import time
import Database
db = Database.Database()
import sys
from Catalog.Schema import DBSchema
from Query.Optimizer import Optimizer
from Query.Optimizer import BushyOptimizer
from Query.Optimizer import GreedyOptimizer


lineitemSchema = db.relationSchema('lineitem')

query1 = db.query().fromTable('lineitem').finalize()
#query1 = db.query().fromTable('lineitem').where('l_shipdate >= 19940101 and l_shipdate < 19950101 and l_discount > 0.05 and l_discount <  0.07 and l_quantity < 24').finalize()


start = time.time()
db.processQuery(query1)
end = time.time()

f = open("1query.txt", "w")
f.write(str(start - end) + " , ")

optQuery = db.optimizer.optimizeQuery(query1)
start = time.time()
db.processQuery(optQuery)
end = time.time()

f.write(str(start - end))

f.close()


#result = db.optimizer.pickJoinOrder(query1)

#query = db.query().fromTable('Iabc').join( \
#  db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
#  db.query().fromTable('Ighi').join( \
#  db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
#  expr='b == i and e == k').finalize()
#result = db.optimizer.pickJoinOrder(query)
#print(result.explain())
#print("\n\n\n")
#
#aggMinMaxSchema = DBSchema('minmax', [('minAge', 'int'), ('maxAge','int')])
#keySchema  = DBSchema('aKey',  [('a', 'int')])
#queryGroup = db.query().fromTable('Iabc').where('a < 20').join( \
#  db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').where('c > f').join( \
#  db.query().fromTable('Ighi').join( \
#  db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
#  expr='b == i and e == k').where('a == h and d == 5').groupBy( \
#  groupSchema=DBSchema('aKey', [('a', 'int')]), \
#  aggSchema=aggMinMaxSchema, \
#  groupExpr=(lambda e: e.a % 2), \
#  aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.b), lambda x: x), \
#  (0, lambda acc, e: max(acc, e.b), lambda x: x)], \
#  groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2) \
#  ).finalize()
#result = db.optimizer.pickJoinOrder(queryGroup)
#print(result.explain())
#print("\n\n\n")
#
#querySelect = db.query().fromTable('Iabc').where('a < 20').join( \
#     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').where('c > f').join( \
#     db.query().fromTable('Ighi').join( \
#     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
#     expr='b == i and e == k').where('a == h and d == 5').finalize()
#result = db.optimizer.pickJoinOrder(querySelect)
#print(result.explain())
#print("\n\n\n")


#  # Join Order Optimization
#query4 = db.query().fromTable('employee').join( \
#  db.query().fromTable('department'), \
#  method='block-nested-loops', expr='id == eid').finalize()
#result = db.optimizer.pickJoinOrder(query4)
#print(result.explain())

## Pusshdown Optimization
#query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
#  db.query().fromTable('department'), \
#  method='block-nested-loops', expr='id == eid')\
#  .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
#  .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()
