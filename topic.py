#!/usr/bin/env python

'''topic

Extensions for eScholarship-style topic branches.
'''

import re
from xml import sax
from StringIO import StringIO
from mercurial import cmdutil, commands, extensions, hg, patch, util
from mercurial.node import nullid, nullrev

#################################################################################
def ruleError(ui, message):
  """ Convenience function: Output message through ui.warn(), and return False. """

  ui.warn("Error: %s\n" % message)
  return False


#################################################################################
def checkTabs(ui, repo, node):

  fp = StringIO()
  patch.export(repo, [node], fp=fp)
  result = fp.getvalue()
  fp.close()
  for line in result.split("\n"):
    if re.match("^\+\s*\t", line):
      return ruleError(ui, "Coding standard does not allow tab characters to be introduced.\n"
              "Please set your editor to expand tabs to spaces, then search for\n"
              "all tabs and replace them.\n"
              "Offending line:\n" + line)

  return True


#################################################################################
def checkXML(ui, repo, node):
  """ Ensure than any XML files being commited are well-formed. """

  ctx = repo[node]
  for filename in ctx:
    data = ctx[filename].data()
    if util.binary(data):
      continue
    if data.startswith("<?xml "):
      try:
        sax.parseString(data, sax.ContentHandler())
      except Exception as e:
        return ruleError(ui, "XML file '%s' is not well formed. Parse error: %s\n" % (filename, str(e)))

  return True


#################################################################################
def checkBranch(ui, repo, node, parent1, parent2):
  """ Ensure that branches meet our topic branching rules. """

  # Collect some useful info
  ctx = repo[node]
  thisBranch = ctx.branch()
  p1Branch = repo[parent1].branch()
  if parent2 == '':
    parent2 = None
  p2Branch = repo[parent2].branch() if parent2 else None

  # Don't allow anything to go into the default branch.
  if thisBranch == 'default':
    return ruleError(ui, "Committing to default branch not allowed. Try making a topic branch.")

  # Don't allow dev/stage/prod branches to be closed.
  if ctx.extra().get('close') and thisBranch in ['dev', 'stage', 'prod']:
    return ruleError(ui, "Not allowed to close the main dev/stage/prod branches.")

  # Prevent multiple heads on the same branch
  if len(repo.branchmap()[thisBranch]) > 1:
    return ruleError(ui, "Not allowed to create multiple heads in the same branch. Did you forget to merge?")

  # New branches may only branch from dev.
  if thisBranch != p1Branch and p1Branch != 'dev':
    return ruleError(ui, "Topics are only allowed to branch from dev directly.")

  isMerge = parent1 and parent2
  if isMerge:

    # Merges cannot go into default
    if thisBranch == 'default':
      return ruleError(ui, "Merging into default branch not allowed.")

    # Merges must come from topic branches
    if p2Branch in ['default', 'dev', 'stage', 'prod']:
      return ruleError(ui, "Merge must come from a topic branch.")

    # Merge to stage must have gone to dev first; prod must have gone to stage.
    reqPred = 'dev' if thisBranch == 'stage' else 'stage' if thisBranch == 'prod' else None
    if reqPred:

      # Determine the last rev of this feature that was merged to the required predecessor.
      # For instance, if we're trying to merge to stage, find the last rev that was merged
      # to dev.
      #
      # TODO: I think this *will* work when processing multiple changegroups during a push,
      #       but gotta test that.
      #
      branchMap = repo.branchmap()
      assert reqPred in branchMap
      reqHead = branchMap[reqPred][0]
      ancestor = repo[reqHead].ancestor(ctx)

      # If there have been changes to the feature since the last merge to the predecessor,
      # that means somebody forgot to do the missing merge.
      #
      if ancestor != repo[parent2]:
        return ruleError(ui, "You must merge changes to %s before %s." % (reqPred, thisBranch))

  else:

    # Non-merge. These plain commits are not allowed on dev/stage/prod.
    if thisBranch in ('dev', 'stage', 'prod'):
      return ruleError(ui, "Direct commits to dev/stage/prod not allowed. Try making a topic branch.")

  return True # no problems.


#################################################################################
def pretxncommit(ui, repo, node, parent1, parent2, **kwargs):

  """ Perform content-specific checks before accepting a commit (clients) or incoming changegroup (servers) """

  # Check for any tabs being added in any file. They're not allowd.
  if not checkTabs(ui, repo, node):
    return True # abort

  # XML must be well-formed
  if not checkXML(ui, repo, node):
    return True # abort

  # Branches must meet our topic rules
  if not checkBranch(ui, repo, node, parent1, parent2):
    return True # abort
  
  # All done.
  return False # no problems found


#################################################################################
def revsOnBranch(repo, branch):
  """ Yield all revisions of the given branch, in topoligical order. """

  visit = repo.branchheads(branch, closed = True)
  seen = set()
  while visit:
    p = visit.pop()

    if p in seen:
      continue
    seen.add(p)

    if type(p) == str:
      p = repo[p]
    if p.branch() != branch:
      continue

    visit.extend(p.parents())
    yield p


#################################################################################
def calcBranchState(repo, branch, ctx):
  """ Figure out whether the given branch is local, or has been merged to
      dev/stage/prod. """

  # Find all the revs in this feature branch
  featureRevs = list(revsOnBranch(repo, branch))

  # Detect if the branch has been closed
  if 'close' in ctx.changeset()[5]:
    states = {ctx: 'closed'}
  else:
    states = {ctx: 'local'}

  # Now check for merges to dev, stage, and prod
  for mainBr in ('dev', 'stage', 'prod'):
    found = None
    for p in revsOnBranch(repo, mainBr):
      for pp in p.parents():
        if pp in featureRevs:
          found = pp
      if found:
        states[found] = mainBr
        break

  # Make the final textual version
  outList = []
  for (p, state) in states.iteritems():
    if p != ctx:
      state += "(%d)" % int(p)
    if p in [repo[p] for p in repo.dirstate.parents()]:
      state = "*" + state
    outList.append(state)

  return ", ".join(outList)


#################################################################################
def tbranch(ui, repo, *args, **kwargs):

  if len(args) < 1:
    ui.status("Current branch: %s\n" % repo.dirstate.branch())
    return

  target = args[0]
  if target == repo.dirstate.branch():
    ui.status("You're already on that branch.\n")
    return

  if target in topicBranchNames(repo, closed=True) + ['dev', 'prod', 'stage']:
    opts = {}
    opts['clean'] = kwargs.get('clean', False)
    #opts['check'] = True
    return commands.update(ui, repo, node=target, check=True)

  if ui.prompt("Create new branch '%s'?" % target) != 'y':
    return 1

  if repo.dirstate.branch() != 'dev':
    ui.status("Branching from dev...\n")
    res = commands.update(ui, repo, node='dev', check=True)
    if res: return res

  return commands.branch(ui, repo, target)


#################################################################################
def printColumns(ui, colNames, rows, indent=0):
  """ Print columnar data. """

  # Add column names at the top
  rows.insert(0, colNames)

  # Determine the max size of each column
  nColumns = len(colNames)
  colSize = [0] * nColumns
  for tup in rows:
    colSize = [max(colSize[i], len(str(tup[i]))) for i in range(nColumns)]

  # Add a separator for the column names
  rows.insert(1, ["-" * colSize[i] for i in range(nColumns)])

  # And print the whole thing
  ui.status("\n")
  for tup in rows:
    ui.status(" "*indent + ("  ".join(["%-*s" % (colSize[i], str(tup[i])) for i in range(nColumns)]) + "\n"))
  ui.status("\n")


#################################################################################
def topicBranches(repo, closed = False):
  """ Get a list of the topic branches, ordered by descending date of last change. 
      Each list entry is a tuple (branchname, headCtx) """

  # Process each branch
  result = []
  for tag, node in repo.branchtags().items():

    # Skip dev/stage/prod
    if tag in ('default', 'dev', 'stage', 'prod'):
      continue

    # Skip closed branches if requested
    hn = repo.lookup(node)
    if not closed:
      if not hn in repo.branchheads(tag, closed=False):
        continue

    # Determine all the field values we're going to print
    result.append((tag, repo[hn]))

  result.sort(lambda a,b: (a[1].date() < b[1].date()) - (a[1].date() > b[1].date()))
  return result


#################################################################################
def topicBranchNames(repo, closed = False):
  """ Return a list of the open topic branches. """

  return [tup[0] for tup in topicBranches(repo, closed)]


#################################################################################
def tbranches(ui, repo, *args, **kwargs):
  """ show recent activity on the currently open topic branches. """

  closed = kwargs.get('closed', False)

  # Process each branch
  toPrint = []
  for (branch, ctx) in topicBranches(repo, closed):

    # Determine all the field values we're going to print
    user = util.shortuser(ctx.user())
    date = util.shortdate(ctx.date())
    branchState = calcBranchState(repo, branch, ctx)

    descrip = ctx.description().splitlines()[0].strip()
    if len(descrip) > 60:
      descrip = descrip[0:57] + "..."

    toPrint.append((branch, user, date, branchState, descrip))

  printColumns(ui, ('Branch', 'User', 'Date', 'State', 'Description'), toPrint)


#################################################################################
def tlog(ui, repo, *pats, **opts):
  """ show revision history of the current branch (or all branches). """

  if opts['all']:
    branches = topicBranchNames(repo)
  else:
    branches = [repo.dirstate.branch()]

  for branch in branches:

    str = "Branch: " + branch
    ui.status("\n%s\n" % str)
    ui.status("%s\n" % ("=" * len(str)))

    toPrint = []

    for id in repo:
      ctx = repo[id]
      kind = None
      if ctx.branch() == branch:
        kind = 'local'
      elif ctx.parents() and ctx.parents()[0].branch() == branch:
        kind = ctx.branch()
      elif len(ctx.parents()) > 1 and ctx.parents()[1].branch() == branch:
        kind = ctx.branch()

      if kind:
        user = util.shortuser(ctx.user())
        date = util.shortdate(ctx.date())
        descrip = ctx.description().splitlines()[0].strip()
        if len(descrip) > 60: descrip = descrip[0:57] + "..."
        toPrint.append((int(ctx), kind, user, date, descrip))

    printColumns(ui, ('Rev num', 'Target', 'User', 'Date', 'Description'), toPrint, indent=4)

#################################################################################
def replacedCommand(orig, ui, *args, **kwargs):
  """ This is called for commands our extension replaces, like "branch". We
      print an informative message, or if the "--tforce" option is specified,
      we go ahead and run the original commands. """

  tforce = kwargs.pop('tforce', None)
  if tforce:
    return orig(ui, *args, **kwargs)
  ui.warn("Command replaced by 'topic-branch' extension.\n"
          "Use tbranch/tbranches/tpush, or else specify --tforce if you really want the original.\n")
  return 1


#################################################################################
def uisetup(ui):
  """ Called by Mercurial to give us a chance to manipulate the ui. """

  # Set replaced commands to print a message unless forced.
  if False:
    overrideOpt = [('', 'tforce', None, "override check and run original hg command")]
    entry = extensions.wrapcommand(commands.table, 'branch', replacedCommand)
    entry[1].extend(overrideOpt)
    entry = extensions.wrapcommand(commands.table, 'branches', replacedCommand)
    entry[1].extend(overrideOpt)
    entry = extensions.wrapcommand(commands.table, 'push', replacedCommand)
    entry[1].extend(overrideOpt)


#################################################################################
# Table of commands we're adding.
#
cmdtable = {
    "tbranch|tb":    (tbranch,
                      [],
                      ""),

    "tbranches|tbs": (tbranches,
                      [('c', 'closed', None, "include closed branches")],
                      ""),

    "tlog":          (tlog,
                      [('a', 'all', None, "all topic branches, not just current")] + commands.templateopts,
                      ""),

}

