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

  # Pushdown Optimization
  >>> query6 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)').finalize()
  >>> print(db.optimizer.pushdownOperators(query6).explain())

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
        for f in currNode.schema().fields:
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

      if len(attrList) == 1: #TODO should really be number of sources, not num attributes
        (pNode, sub) = fieldDict[attrList.pop()]
        if sub == "only":
          s.subPlan = pNode.subPlan
          pNode.subPlan = s
        elif sub == "left":
          s.subPlan = pNode.lhsPlan
          pNode.lhsPlan = s
        elif sub == "right":
          s.subPlan = pNode.rhsPlan
          pNode.rhsPlan = s
        else:
          s.subPlan = removedPlan.root
          removedPlan.root = s
      else:
        #TODO handle selects with multiple attributes (and dealing with projects)
        s.subPlan = removedPlan.root
        removedPlan.root = s
      
    return removedPlan
    
  
  def getLeg(self, relId, plan):
    currPlan = plan

    while (len(currPlan.relations()) > 1) and (relId in currPlan.relations()):
      currNode = currPlan.root
      if len(currNode.inputs()) > 1:
        if relId in currNode.lhsPlan.relations():
          currPlan = currNode.lhsPlan
        else:
          currPlan = currNode.rhsPlan        
      else:
        currPlan = currNode.subPlan

    if relId in currPlan.relations():
      return currPlan

    return None  


  def obtainFieldDict(self, plan):
    #TODO implement. Should map attr name -> relId
    raise NotImplementedError

  def getExprDicts(self,plan, fieldDict):
    q = []
    q.append(plan.root)
    selectTablesDict = {} # mapping of relation list to list of exprs using them: [A,B] -> [a < b, etc]
    JoinTablesDict = {} # same thing but for joins, not selects 

    while len(q) > 0:
      currNode = q.pop()

      if (currNode.operatorType() == "Select"):
        #all selects were already decomposed in pushdown
        attrList = ExpressionInfo(currNode.selectExpr).getAttributes()
        sourceList = [] #TODO this approach should be used in pushdown also
        for attr in attrList: #Could be more than 2! (a<b or c>1)
          source = fieldDict[attr]          #TODO ^ check we didnt make a poor assumption somewhere else
          if source not in sourceList:
            sourceList.append(source)

        sourceList.sort()
        if sourceList not in selectTablesDict:
          selectTablesDict[sourceList] = []
        selectTablesDict[sourceList].append(currNode.selectExpr)
 
      elif currNode.operatorType() == "Join":
        # TODO (what if some join exprs are from BNLJ and others from Hash?

        joinExprList = ExpressionInfo(currNode.joinExpr).decomposeCNF()
        for joinExpr in joinExprList:
          attrList = ExpressionInfo(joinExpr).getAttributes()
          sourceList = [] #TODO this approach should be used in pushdown also
          for attr in attrList: #Could be more than 2! (a<b or c>1)
            source = fieldDict[attr]          #TODO ^ check we didnt make a poor assumption somewhere else
            if source not in sourceList:
              sourceList.append(source)

          sourceList.sort()
          if sourceList not in joinTablesDict:
            joinTablesDict[sourceList] = []
          joinTablesDict[sourceList].append(currNode.selectExpr)
        

      if len(currNode.inputs()) > 1:
        q.append(currNode.lhsPlan)
        q.append(currNode.rhsPlan)
      else:
        q.append(currNode.subPlan)


    return (joinTablesDict, selectTablesDict)


  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    relations = plan.relations()
    fieldDict = self.obtainFieldDict(plan)
    (joinTablesDict, selectTablesDict) = self.getExprDicts(plan, fieldDict)
    # makes dicts that maps a list of relations to exprs involving that list
    # then in system R we will build opt(A,B) Join C using join exprs involving A,C and B,C
    # and on top of it the select exprs that involve 2 tables A,C or B,C

    for npass in range(len(relations)):
      npass += 1
      if npass == 1: #pass 1 case
        for rel in relations:
          self.statsCache[rel] = self.getLeg(rel, plan) 
      else:
        #TODO pass n case
        pass

      
  #TODO perhaps combine several of the pre-traversals into one function that finds everything out about
  # the input plan in one traversal and call it at start of optimize query, passing info into push/reorder
  # or maybe we do this approach but with 2 traversal, one that we use in pushdown to get all we need there
  # and one for join ordering

  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
