import Database
db = Database.Database()
import sys
from Catalog.Schema import DBSchema
from Query.Optimizer import Optimizer
from Query.Optimizer import BushyOptimizer
from Query.Optimizer import GreedyOptimizer
try:
  db.createRelation('department', [('did', 'int'), ('eid', 'int'), ('dummy', 'int')])
  db.createRelation('employee', [('id', 'int'), ('age', 'int'), ('stupid', 'int')])
  db.createRelation('Iabc', [('a', 'int'), ('b', 'int'), ('c', 'int')])
  db.createRelation('Idef', [('d', 'int'), ('e', 'int'), ('f', 'int')])
  db.createRelation('Ighi', [('g', 'int'), ('h', 'int'), ('i', 'int')])
  db.createRelation('Ijkl', [('j', 'int'), ('k', 'int'), ('l', 'int')])
  db.createRelation('Imno', [('m', 'int'), ('n', 'int'), ('o', 'int')])
  db.createRelation('Ipqr', [('p', 'int'), ('q', 'int'), ('r', 'int')])
  db.createRelation('Istu', [('s', 'int'), ('t', 'int'), ('u', 'int')])
  db.createRelation('Ivwx', [('v', 'int'), ('w', 'int'), ('x', 'int')])
except ValueError:
  pass

depSchema = db.relationSchema('department')
empSchema = db.relationSchema('employee')
iabcSchema = db.relationSchema('Iabc')
idefSchema = db.relationSchema('Idef')
ighiSchema = db.relationSchema('Ighi')
ijklSchema = db.relationSchema('Ijkl')

schemaList1 = [iabcSchema, ighiSchema]
schemaList = [depSchema, empSchema, iabcSchema, idefSchema, ighiSchema, ijklSchema]

for s in schemaList:
  for tup in [s.pack(s.instantiate(i, 2*i+20, i % 2)) for i in range(10)]:
    db.insertTuple(s.name, tup)

#for s in schemaList1:
#  for tup in [s.pack(s.instantiate(i, 2*i+20, i % 2)) for i in range(11,30)]:
#    db.insertTuple(s.name, tup)

query = db.query().fromTable('Iabc').join( \
  db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
  db.query().fromTable('Ighi').join( \
  db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
  expr='b == i and e == k').finalize()
result = db.optimizer.pickJoinOrder(query)
print(result.explain())
print("\n\n\n")

aggMinMaxSchema = DBSchema('minmax', [('minAge', 'int'), ('maxAge','int')])
keySchema  = DBSchema('aKey',  [('a', 'int')])
queryGroup = db.query().fromTable('Iabc').where('a < 20').join( \
  db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').where('c > f').join( \
  db.query().fromTable('Ighi').join( \
  db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
  expr='b == i and e == k').where('a == h and d == 5').groupBy( \
  groupSchema=DBSchema('aKey', [('a', 'int')]), \
  aggSchema=aggMinMaxSchema, \
  groupExpr=(lambda e: e.a % 2), \
  aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.b), lambda x: x), \
  (0, lambda acc, e: max(acc, e.b), lambda x: x)], \
  groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2) \
  ).finalize()
result = db.optimizer.pickJoinOrder(queryGroup)
print(result.explain())
print("\n\n\n")

querySelect = db.query().fromTable('Iabc').where('a < 20').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').where('c > f').join( \
     db.query().fromTable('Ighi').join( \
     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
     expr='b == i and e == k').where('a == h and d == 5').finalize()
result = db.optimizer.pickJoinOrder(querySelect)
print(result.explain())
print("\n\n\n")


  # Join Order Optimization
query4 = db.query().fromTable('employee').join( \
  db.query().fromTable('department'), \
  method='block-nested-loops', expr='id == eid').finalize()
result = db.optimizer.pickJoinOrder(query4)
print(result.explain())

# Pusshdown Optimization
query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
  db.query().fromTable('department'), \
  method='block-nested-loops', expr='id == eid')\
  .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
  .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()
