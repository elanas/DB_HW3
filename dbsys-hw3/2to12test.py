import Database
db = Database.Database()
import sys
from Catalog.Schema import DBSchema
from Query.Optimizer import Optimizer
from Query.Optimizer import BushyOptimizer
from Query.Optimizer import GreedyOptimizer
try:
  db.createRelation('Iabc', [('a', 'int'), ('b', 'int'), ('c', 'int')])
  db.createRelation('Idef', [('d', 'int'), ('e', 'int'), ('f', 'int')])
  db.createRelation('Ighi', [('g', 'int'), ('h', 'int'), ('i', 'int')])
  db.createRelation('Ijkl', [('j', 'int'), ('k', 'int'), ('l', 'int')])
  db.createRelation('Imno', [('m', 'int'), ('n', 'int'), ('o', 'int')])
  db.createRelation('Ipqr', [('p', 'int'), ('q', 'int'), ('r', 'int')])
  db.createRelation('Istu', [('s', 'int'), ('t', 'int'), ('u', 'int')])
  db.createRelation('Ivwx', [('v', 'int'), ('w', 'int'), ('x', 'int')])
  db.createRelation('Kabc', [('ka', 'int'), ('kb', 'int'), ('kc', 'int')])
  db.createRelation('Kdef', [('kd', 'int'), ('ke', 'int'), ('kf', 'int')])
  db.createRelation('Kghi', [('kg', 'int'), ('kh', 'int'), ('ki', 'int')])
  db.createRelation('Kjkl', [('kj', 'int'), ('kk', 'int'), ('kl', 'int')])
except ValueError:
  pass

iabcSchema = db.relationSchema('Iabc')
idefSchema = db.relationSchema('Idef')
ighiSchema = db.relationSchema('Ighi')
ijklSchema = db.relationSchema('Ijkl')
imnoSchema = db.relationSchema('Imno')
ipqrSchema = db.relationSchema('Ipqr')
istuSchema = db.relationSchema('Istu')
ivwxSchema = db.relationSchema('Ivwx')
kabcSchema = db.relationSchema('Kabc')
kdefSchema = db.relationSchema('Kdef')
kghiSchema = db.relationSchema('Kghi')
kjklSchema = db.relationSchema('Kjkl')




schemaList = [ iabcSchema, idefSchema, ighiSchema, ijklSchema, imnoSchema, ipqrSchema, istuSchema, ivwxSchema, kabcSchema, kdefSchema, kghiSchema, kjklSchema ]
#arbitratily half of the tables will have more tuples so it's not so symmetrical
schemaList1 = [ iabcSchema, ighiSchema, imnoSchema, istuSchema, kabcSchema, kghiSchema ]


for s in schemaList:
  print(str(s))
  for tup in [s.pack(s.instantiate(i, 2*i+20, i % 2)) for i in range(10)]:
    db.insertTuple(s.name, tup)

for s in schemaList1:
  for tup in [s.pack(s.instantiate(i, 2*i+20, i % 2)) for i in range(11,30)]:
    db.insertTuple(s.name, tup)


query2 = db.query().fromTable('Iabc').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').finalize()

query4 =  db.query().fromTable('Iabc').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
     db.query().fromTable('Ighi'), method='block-nested-loops', expr='a == g').join( \
     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='a == j').finalize()

query6 =  db.query().fromTable('Iabc').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
     db.query().fromTable('Ighi'), method='block-nested-loops', expr='a == g').join( \
     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='a == j').join( \
     db.query().fromTable('Imno'), method='block-nested-loops', expr='a == m').join( \
     db.query().fromTable('Ipqr'), method='block-nested-loops', expr='a == p').finalize()

query8 =  db.query().fromTable('Iabc').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
     db.query().fromTable('Ighi'), method='block-nested-loops', expr='a == g').join( \
     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='a == j').join( \
     db.query().fromTable('Imno'), method='block-nested-loops', expr='a == m').join( \
     db.query().fromTable('Ipqr'), method='block-nested-loops', expr='a == p').join( \
     db.query().fromTable('Istu'), method='block-nested-loops', expr='a == s').join( \
     db.query().fromTable('Ivwx'), method='block-nested-loops', expr='a == v').finalize()

query10 =  db.query().fromTable('Iabc').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
     db.query().fromTable('Ighi'), method='block-nested-loops', expr='a == g').join( \
     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='a == j').join( \
     db.query().fromTable('Imno'), method='block-nested-loops', expr='a == m').join( \
     db.query().fromTable('Ipqr'), method='block-nested-loops', expr='a == p').join( \
     db.query().fromTable('Istu'), method='block-nested-loops', expr='a == s').join( \
     db.query().fromTable('Ivwx'), method='block-nested-loops', expr='a == v').join( \
     db.query().fromTable('Kabc'), method='block-nested-loops', expr='a == ka').join( \
     db.query().fromTable('Kdef'), method='block-nested-loops', expr='a == kd').finalize()


query12 =  db.query().fromTable('Iabc').join( \
     db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
     db.query().fromTable('Ighi'), method='block-nested-loops', expr='a == g').join( \
     db.query().fromTable('Ijkl'), method='block-nested-loops', expr='a == j').join( \
     db.query().fromTable('Imno'), method='block-nested-loops', expr='a == m').join( \
     db.query().fromTable('Ipqr'), method='block-nested-loops', expr='a == p').join( \
     db.query().fromTable('Istu'), method='block-nested-loops', expr='a == s').join( \
     db.query().fromTable('Ivwx'), method='block-nested-loops', expr='a == v').join( \
     db.query().fromTable('Kabc'), method='block-nested-loops', expr='a == ka').join( \
     db.query().fromTable('Kdef'), method='block-nested-loops', expr='a == kd').join( \
     db.query().fromTable('Kghi'), method='block-nested-loops', expr='a == kg').join( \
     db.query().fromTable('Kjkl'), method='block-nested-loops', expr='a == kj').finalize()



db.optimizer.optimizeQuery(query2)
print("2 done")
db.optimizer.optimizeQuery(query4)
print("4 done")
db.optimizer.optimizeQuery(query6)
print("6 done")
db.optimizer.optimizeQuery(query8)
print("8 done")
db.optimizer.optimizeQuery(query10)
print("10 done")
db.optimizer.optimizeQuery(query12)
print("12 done")

