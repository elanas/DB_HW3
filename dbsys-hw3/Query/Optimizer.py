import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  We provide doctests for example usage only.
  Implementations and cost heuristics may vary.

  >>> import Database
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  ... except ValueError:
  ...   pass

  # Join Order Optimization
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  # Pushdown Optimization
  >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

  >>> db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    raise NotImplementedError

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    raise NotImplementedError

  def removeUnaryPlan(self, plan):
    fieldDict = {}
    selectList = []
    q = []
    q.append((plan.root,None, ""))

    while len(q) > 0:
      (currNode, pNode, sub) = q.pop()
      if currNode.operatorType() == "Select":
        selectList.append(currNode)
        q.append((currNode.subPlan, currNode, "only"))
        if sub == "only":
          pNode.subPlan = currNode.subPlan
        elif sub == "left":
          pNode.lhsPlan = currNode.subPlan
        elif sub == "right":
          pNode.rhsPlan = currNode.subPlan
        else:
          plan.root = currNode.subPlan
      elif currNode.operatorType() == "Project":
        #TODO add implementation
        continue
      elif currNode.operatorType() == "TableScan":
        for f in currNode.schema.fields:
          fieldDict[f] = (pNode,sub)
        continue
      elif currNode.operatorType() == "GroupBy" or currNode.operatorType() == "Sort":
        q.append((currNode.subPlan, currNode, "only"))
      else: #join and union
        q.append((currNode.lhsPlan, currNode, "left"))
        q.append((currNode.rhsPlan, currNode, "right"))
    
    return (plan,selectList,fieldDict)

  def decompSelects(self,selectList):
    decompList = []

    for s in selectList:
      exprList = ExpressionInfo(s.selectExpr).decomposeCNF()
      for e in exprList:
        select = Select(None,e)
        decompList.append(select)
    return decompList
  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    (removedPlan,selectList,fieldDict) = self.removeUnaryPlan(plan)
    decompList = self.decompSelects(selectList)

    for s in decompList:
      attrList = ExpressionInfo(s.selectExpr).getAttributes()
      if len(attrList) == 1:
        (pNode, sub) = fieldDict[attrList[0]]
        if sub == "only":
          pNode.subPlan = s
        elif sub == "left":
          pNode.lhsPlan = s
        elif sub == "right":
          pNode.rhsPlan = s
        else:
          removedPlan.root = s
      else:
        #TODO handle selects with multiple attributes (and dealing with projects)
        removedPlan.root = s
      
      return removedPlan
    

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    raise NotImplementedError

  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
