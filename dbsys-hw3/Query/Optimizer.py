import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.TableScan import TableScan 
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
  ...   db.createRelation('Iabc', [('a', 'int'), ('b', 'int'), ('c', 'int')])
  ...   db.createRelation('Idef', [('d', 'int'), ('e', 'int'), ('f', 'int')])
  ...   db.createRelation('Ighi', [('g', 'int'), ('h', 'int'), ('i', 'int')])
  ...   db.createRelation('Ijkl', [('j', 'int'), ('k', 'int'), ('l', 'int')])
  ...   db.createRelation('Imno', [('m', 'int'), ('n', 'int'), ('o', 'int')])
  ...   db.createRelation('Ipqr', [('p', 'int'), ('q', 'int'), ('r', 'int')])
  ...   db.createRelation('Istu', [('s', 'int'), ('t', 'int'), ('u', 'int')])
  ...   db.createRelation('Ivwx', [('v', 'int'), ('w', 'int'), ('x', 'int')])
  ... except ValueError:
  ...   pass

  >>> query = db.query().fromTable('Iabc').join( \
        db.query().fromTable('Idef'), method='block-nested-loops', expr='a == d').join( \
        db.query().fromTable('Ighi').join( \
        db.query().fromTable('Ijkl'), method='block-nested-loops', expr='h ==  j'), method='block-nested-loops', \
        expr='b == i and e == k').finalize()
  >>> result = db.optimizer.pickJoinOrder(query)
  >>> print(result.explain())


  # Join Order Optimization
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()
  >>> result = db.optimizer.pickJoinOrder(query4)
  >>> print(result.explain())

  # Pushdown Optimization
  >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()


  # Pushdown Optimization
 # >>> query6 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
 #       db.query().fromTable('department'), \
 #       method='block-nested-loops', expr='id == eid')\
 #       .where('eid > 0 and id > 0 and (eid == 5 or id == 6)').finalize()
 # >>> print(db.optimizer.pickJoinOrder(query6).explain())

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
    
  def obtainFieldDict(self, plan):
    q = []
    q.append(plan.root)
    
    attrDict = {}

    while len(q) > 0:
      currNode = q.pop()

      if currNode.operatorType() == "TableScan":
        for f in currNode.schema().fields:
          attrDict[f] = currNode.relationId()

      for i in currNode.inputs():
        q.append(i)
    
    return attrDict    

  def getExprDicts(self, plan, fieldDict):
    q = []
    q.append(plan.root)
    selectTablesDict = {} # mapping of relation list to list of exprs using them: [A,B] -> [a < b, etc]
    joinTablesDict = {} # same thing but for joins, not selects 

    f = open("stop.txt", "a")

    while len(q) > 0:
      currNode = q.pop()
      if (currNode.operatorType() == "Select"):
        #all selects were already decomposed in pushdown #TODO this isn't true!
        attrList = ExpressionInfo(currNode.selectExpr).getAttributes()
        sourceList = [] 
        for attr in attrList: #Could be more than 2! (a<b or c>1)
          source = fieldDict[attr]          #TODO ^ check we didnt make a poor assumption somewhere else
          if source not in sourceList:
            sourceList.append(source)

        sourceTuple = tuple(sorted(sourceList))
        if sourceTuple not in selectTablesDict:
          selectTablesDict[sourceTuple] = []
        selectTablesDict[sourceTuple].append(currNode.selectExpr)
 
      elif "Join" in currNode.operatorType():
        joinExprList = ExpressionInfo(currNode.joinExpr).decomposeCNF()
        for joinExpr in joinExprList:
          attrList = ExpressionInfo(joinExpr).getAttributes()
          sourceList = [] 
          for attr in attrList: #Could be more than 2! (a<b or c>1)
            source = fieldDict[attr]          #TODO ^ check we didnt make a poor assumption somewhere else
            if source not in sourceList:
              sourceList.append(source)

          sourceTuple = tuple(sorted(sourceList))
          if sourceTuple not in joinTablesDict:
            joinTablesDict[sourceTuple] = []
          joinTablesDict[sourceTuple].append(currNode.joinExpr)
        

      if len(currNode.inputs()) > 1:
        q.append(currNode.lhsPlan)
        q.append(currNode.rhsPlan)
      elif len(currNode.inputs()) == 1:
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

    optDict = {}

    for npass in range(1, len(relations) + 1):
      if npass == 1:
        for r in relations:
          table = TableScan(r,self.db.relationSchema(r))
          if (r,) in selectTablesDict: 
            selectExprs = selectTablesDict[(r,)]
            selectString = combineSelects(selectExprs)
            select = Select(table,selectString)
            optDict[(r,)] = Plan(root=select)
          else:
            optDict[(r,)] = Plan(root=table)
      else:
        combinations = itertools.combinations(relations,npass)
        for c in combinations:
          clist = sorted(c)
          bestJoin = None
          for rel in clist:
            temp = list(clist)
            temp.remove(rel)
            leftOps = optDict[tuple(temp)].root
            rightOps = optDict[(rel,)].root

            joinExpr = self.createExpression(temp, [rel], joinTablesDict)
            joinBnlj = Plan(root=Join(leftOps, rightOps, expr=joinExpr, method="block-nested-loops"))
            joinBnlj.prepare(self.db)
            joinBnlj.sample(100)
            joinNlj = Plan(root=Join(leftOps, rightOps, expr=joinExpr, method="nested-loops"))
            joinNlj.prepare(self.db)
            joinNlj.sample(100)

            if joinBnlj.cost(True) < joinNlj.cost(True):
              if bestJoin == None or joinBnlj.cost(True) < bestJoin.cost(True):
                bestJoin = joinBnlj
            else:
              if bestJoin == None or joinNlj.cost(True) < bestJoin.cost(True):
                bestJoin = joinNlj
          optDict[tuple(clist)] = bestJoin

    # after System R algorithm
    

  def createExpression(self, lList, rList, exprDict):
   
    lfile = open("lfile.txt","w")
    lfile.write(str(lList) + " " + str(rList))
    lfile.close()
    lcombos = []
    lTemp = []
    rcombos = []
    rTemp = []
    for i in range(1, len(lList) + 1):
      lTemp.extend(itertools.combinations(lList,i))
    lcombos = [list(elem) for elem in lTemp]
    for i in range(1, len(rList) + 1):
      rTemp.extend(list(itertools.combinations(rList,i)))
    rcombos = [list(elem) for elem in rTemp]
    plist = list(itertools.product(lcombos,rcombos))
   
    f = open("dict.txt", "w")
    #f.write(str(exprDict))
    #f.write("----")
    #f.write(str(lcombos) + " " + str(rcombos))
   
    #masterlist = [tuple(sorted(elem[0].extend(elem[1]))) for elem in plist]
    masterlist = []

    for elem in plist:
      item1 = elem[0]
      item2 = elem[1]
      item1.extend(item2)
      masterlist.append(sorted(item1))
      

    f.write(str(masterlist))      
    f.close()
    
    #masterlist = plist

    exprString = ""
    
   
    for listc in masterlist:
      c = tuple(listc)
      if c in exprDict:
        for s in exprDict[c]:
          exprString += s + " and "
    
    if(exprString == ""):
      return "True"
    exprString = exprString[:len(exprString) - 5]
    
    return exprString

  def combineSelects(self,selectExprs):
    ##TODO: we could sort selects to order based on Selectivity
    selectString = ""
    for s in selectExprs:
      selectString += s
      selectString += " and "

    selectString = selectString[:len(selectString) - 5]
    return selectString

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
