import time
import Database
db = Database.Database()
import sys
from Catalog.Schema import DBSchema
from Query.Optimizer import Optimizer
from Query.Optimizer import BushyOptimizer
from Query.Optimizer import GreedyOptimizer


lineitemSchema = db.relationSchema('lineitem')
partSchema = db.relationSchema('part')

# need to specify a group schema, so need this dummy column
query1 = db.query().fromTable('lineitem').where( \
'L_SHIPDATE >= 19940101 and L_SHIPDATE < 19950101 and L_DISCOUNT > 0.05 and L_DISCOUNT <  0.07 and L_QUANTITY < 24').groupBy( \
  groupSchema=DBSchema('dummy', [('a', 'int')]), \
  aggSchema=DBSchema('rev', [('revenue', 'double')]), \
  groupExpr=(lambda e: 1), \
  aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)], \
  groupHashFn=(lambda e: 1) \
).finalize()

query2 = db.query().fromTable('lineitem').join(db.query().fromTable('part'), method='block-nested-loops', expr='L_PARTKEY == P_PARTKEY' \
).where('L_SHIPDATE >= 19950901 and L_SHIPDATE < 19951001' \
).groupBy(
  groupSchema=DBSchema('dummy', [('a', 'int')]), \
  aggSchema=DBSchema('rev', [('promo_revenue', 'double')]), \
  groupExpr=(lambda e: 1), \
  aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
  groupHashFn=(lambda e: 1) \
).finalize()


query3 = db.query().fromTable('orders').join(db.query().fromTable('customer'), method='block-nested-loops', \
  expr='C_CUSTKEY == O_CUSTKEY').join(db.query().fromTable('lineitem'), method='block-nested-loops', \
  expr='L_ORDERKEY == O_ORDERKEY').where("C_MKTSEGMENT == 'BUILDING' and O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315" \
).groupBy( \
  groupSchema=DBSchema('group', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')]), \
  aggSchema=DBSchema('sum', [('revenue', 'double')]), \
  groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)), \
  aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
  groupHashFn=(lambda e: e[0] % 10) \
).finalize() 


#print(query1.schema().toString())

testquery = query3


start = time.time()
db.processQuery(testquery)
end = time.time()

f = open("1query.txt", "w")
f.write(str(end - start) + " , ")

optQuery = db.optimizer.optimizeQuery(testquery)
start = time.time()
db.processQuery(optQuery)
end = time.time()

#print(query1.explain())
#print(" ")
#print(optQuery.explain())

f.write(str(end - start))

f.close()


##result = db.optimizer.pickJoinOrder(query1)

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
