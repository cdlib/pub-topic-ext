#!/usr/bin/env python

'''topic

Extensions for eScholarship-style topic branches.
'''

import copy, datetime, os, re, subprocess, sys, tempfile, time
from xml import sax
from StringIO import StringIO
from mercurial import cmdutil, commands, context, dispatch, extensions, context, hg, patch, url, util
from mercurial.node import nullid, nullrev

# Changed API in Mercurial 4.7
try:
  from mercurial import utils
  def shortuser(ui, user):
    return ui.shortuser(user)
  shortdate = utils.dateutil.shortdate
except ImportError:
  def shortuser(ui, user):
    return util.shortuser(user)
  shortdate = util.shortdate

global origCalcChangectxAncestor

topicVersion = "2.4.10"

topicState = {}

cmdtable = {}

try:
  from mercurial import registrar, scmutil
  command = registrar.command(cmdtable)
  revsymbol = scmutil.revsymbol
  isbinary = util.stringutil.binary
except:
  command = cmdutil.command(cmdtable)
  def revsymbol(repo, branch):
    return branch
  isbinary = util.binary

#################################################################################
def ruleError(ui, message):
  """ Convenience function: Output message through ui.warn(), and return False. """

  ui.warn("Error: %s\n" % message)
  return False


#################################################################################
def isTopicRepo(repo):
  """ Check if the given repo is likely a topic repo. """

  bm = repo.branchmap()
  return repo.topicProdBranch in bm


#################################################################################
def mustBeTopicRepo(repo):
  """ Abort if the given repo isn't a topic repo. """

  if not isTopicRepo(repo):
    raise util.Abort("This doesn't appear to be a topic repository.\n" +
                     "If you think it should be, add a 'prod' branch.\n")


#################################################################################
def checkXML(ui, repo, node):
  """ Ensure than any XML files being commited are well-formed. """

  ctx = repo[node]
  status = repo.status(ctx.parents()[0].node(), ctx.node())
  changedOrAdded = status[0] + status[1]
  for filename in changedOrAdded:
    data = ctx[filename].data()
    if isbinary(data):
      continue
    if data.startswith("<?xml ") and not re.search("\.dtd", filename):
      try:
        # Get rid of DTD declaration, since we don't care if it exists or not.
        data = re.sub("<!DOCTYPE [^>]*>", "", data)

        # Now try to parse it
        sax.parseString(data, sax.ContentHandler())

      # Inform the user if anything goes wrong, and reject the file.
      except Exception as e:
        msg = str(e)
        msg = msg.replace("<unknown>:", "location ")
        return ruleError(ui, "XML file '%s' may not be well formed. %s: %s\n" % \
          (filename, e.__class__.__name__, msg))

  return True


#################################################################################
def checkBranch(ui, repo, node):
  """ Ensure that branches meet our topic branching rules. """

  # Collect some useful info
  ctx = repo[node]
  parent1 = ctx.parents()[0]
  parent2 = None if len(ctx.parents()) == 1 else ctx.parents()[1]
  thisBranch = ctx.branch()
  p1Branch = parent1.branch()
  p2Branch = parent2.branch() if parent2 else None

  # Don't allow anything to go into the default branch.
  if thisBranch in repo.topicIgnoreBranches:
    return ruleError(ui, "Committing to branch '%s' not allowed. Try making a topic branch." % thisBranch)

  # Don't allow prod branch to be closed.
  if ctx.extra().get('close') and thisBranch in repo.topicSpecialBranches:
    return ruleError(ui, "Not allowed to close the main production branch.")

  # Prevent multiple heads on the same branch
  if len(repo.branchmap()[thisBranch]) > 1:
    return ruleError(ui, "Not allowed to create multiple heads in the same branch. Did you forget to merge?")

  if thisBranch != p1Branch and p1Branch != repo.topicProdBranch:

    # Previously we allowed branching from dev; no longer.
    if shortdate(ctx.date()) >= "2010-07-07":
      return ruleError(ui, "Topics are only allowed to branch from '%s' directly." % repo.topicProdBranch)

  # New branches may only branch from prod
  if thisBranch not in (p1Branch, p2Branch) and (p1Branch, p2Branch) != (repo.topicProdBranch, None):
    return ruleError(ui, "Topics are only allowed to branch from '%s' directly." % repo.topicProdBranch)

  isMerge = parent1 and parent2
  if isMerge:

    # Merges cannot go into default
    if thisBranch in repo.topicIgnoreBranches:
      return ruleError(ui, "Merging into default branch not allowed.")

    # Merges must come from topic branches or prod
    if p2Branch in repo.topicSpecialBranches and p2Branch != repo.topicProdBranch:
      return ruleError(ui, "Merge must come from a topic branch or prod.")

  else:

    # Non-merge. These plain commits are not allowed on prod.
    if thisBranch in repo.topicSpecialBranches:
      return ruleError(ui, ("Direct commits to '%s' not allowed. Try making a topic branch. " +
        "You may be able to put your uncommitted work into a branch this way: 'hg branch <yourNewName>'") % thisBranch)

  return True # no problems.


#################################################################################
def validateChangegroup(ui, repo, hooktype, **opts):
  """ pretxnchangegroup hook: perform content-specific checks before accepting 
      an incoming changegroup. """

  # Silently skip this for non-topic repos
  if not isTopicRepo(repo):
    return 0

  # If the committer knows what s/he is doing, allow this to go through without
  # any checks.
  #
  for ctx in [repo[n] for n in range(repo[opts['node']].rev(), len(repo))]:
    if "NO_TOPIC_VALIDATE" in ctx.description():
      return 0 # no problems

  # Check each incoming commit to make sure it meets our rules
  for ctx in [repo[n] for n in range(repo[opts['node']].rev(), len(repo))]:
    parents = ctx.parents()
    if len(parents) == 1:
      parents.append(None)
    res = validateCommit(ui, repo, ctx, parents[0], parents[1])
    if res:
      return res

  # No problems found.
  return 0


#################################################################################
def repushChangegroup(ui, repo, hooktype, **opts):
  """ changegroup hook: if pushing to any branch, and configured to do so, 
      push the stuff further on to the appropriate server. """

  # Silently skip this for non-topic repos
  if not isTopicRepo(repo):
    return 0

  # Figure out all the branches that were affected by the changesets.
  branchesChanged = set()
  for ctx in [repo[n] for n in range(repo[opts['node']].rev(), len(repo))]:
    branchesChanged.add(ctx.branch())

  # Now check if any "repush-XYZ" config entries are present, where XYZ is
  # a matching branch name.
  #
  targets = set()
  for branch in branchesChanged:
    str = ui.config('topic', 'repush-'+branch)
    if str:
      ui.status("Re-push to '%s' triggered by %s branch\n" % (repushTargets, branch))
      targets.update(set(re.split("\s*,\s*", repushTargets)))

  # Also, if there's a "repush-always" entry, take it.
  str = ui.config('topic', 'repush-always')
  if len(branchesChanged) > 0 and str:
    ui.status("Re-push to '%s' triggered by repush-always\n" % str)
    targets.update(set(re.split("\s*,\s*", str)))

  # Optionally, some targets can be repushed asynchronously (meaning we won't wait
  # for them to finish).
  #
  str = ui.config('topic', 'repush-async')
  if str:
    asyncTargets = set(re.split("\s*,\s*", str))
  else:
    asyncTargets = set()

  # Push to each targeted repository
  repoDir = os.path.dirname(repo.path)
  hgCmd = ui.config('topic', 'hg-command')
  if hgCmd is None:
    hgCmd = os.path.abspath(sys.argv[0])
  for target in sorted(targets):
    if target in asyncTargets:

      # Asynchronous mode
      ui.status("Re-pushing asynchronously to target %s:\n" % target)
      ui.status('  hg -R "%s" push -f %s *\n' % (repoDir, quoteBranch(target)))

      # Under RhodeCode on Linux there simply doesn't seem to be a way to make
      # a detached process without leaving zombies galore. Instead, we rely on
      # having a cron job that starts a watcher. We communicate with the watcher
      # through temp files.
      #
      tmpDir = os.path.join(os.getenv("HOME"), ".topic_asyncPushes")
      if not os.path.exists(tmpDir):
        os.mkdir(tmpDir)
      (tmpHandle, tmpFile) = tempfile.mkstemp(dir=tmpDir)
      os.close(tmpHandle)
      with open(tmpFile, "w") as f:
        f.write('%s -R "%s" push -f %s\n' % (hgCmd, repoDir, quoteBranch(target)))

    else:

      # Regular synchronous mode.
      ui.status("Re-pushing to target %s:\n" % target)
      if tryCommand(ui, '-R "%s" push -f %s' % (repoDir, quoteBranch(target)), \
                    lambda:os.system('%s -R "%s" push -f "%s"' % \
                                     (hgCmd, repoDir, target))):
        return 1
      ui.status("Done re-pushing to target %s\n" % target)

#################################################################################
def autoUpdate(ui, repo, hooktype, **opts):
  """ changegroup hook: if a push arrives with changes to the current branch,
      update it automatically. """

  # Silently skip this for non-topic repos
  if not isTopicRepo(repo):
    return 0

  # See if any of the changesets affects our current branch
  thisBranch = repo.dirstate.branch()
  needUpdate = False
  for ctx in [repo[n] for n in range(repo[opts['node']].rev(), len(repo))]:
    if ctx.branch() == thisBranch:
      needUpdate = True
  
  # If changes to our branch were found, do an update.
  if needUpdate:
    ui.status("Auto-update on branch %s\n" % thisBranch)
    commands.update(ui, repo, node = thisBranch)
    ui.status("Done with auto-update on branch %s\n" % thisBranch)


#################################################################################
def validateCommit(ui, repo, node, *args, **kwargs):
  """ pretxncommit hook: perform content-specific checks before accepting 
      a commit """

  # Silently skip this for non-topic repos
  if not isTopicRepo(repo):
    return 0

  # If the committer knows what s/he is doing, allow this to go through without
  # any checks.
  #
  ctx = repo[node]
  if "NO_TOPIC_VALIDATE" in ctx.description():
    return False # no problems

  # XML must be well-formed
  if not checkXML(ui, repo, node):
    return True # abort

  # Branches must meet our topic rules
  if not checkBranch(ui, repo, node):
    return True # abort
  
  # All done.
  return False # no problems found


#################################################################################
def revsOnBranch(repo, branch):
  """ Yield all revision nodes of the given branch, in topological order. """

  visit = repo.branchheads(branch, closed = True)
  seen = set()
  while visit:
    p = visit.pop()

    if p in seen:
      continue
    seen.add(p)

    if repo.changelog.read(p)[5].get("branch") != branch:
      continue

    visit.extend(repo.changelog.parents(p))
    yield p

#################################################################################
def findMainMerge(repo, topicRevs, mainBranch):
  """ Figure out whether a local branch has been merged to a main branch. """

  # Handle brand new branch
  if not topicRevs:
    return None

  # Determine where to stop the scan
  firstTopicRev = repo.changelog.rev(topicRevs[-1])

  # Scan for merges from the topic branch to the given main branch
  topicSet = set(topicRevs)
  for mr in revsOnBranch(repo, mainBranch):

    # Stop if we reach a point in history before the start of the topic
    if repo.changelog.rev(mr) < firstTopicRev:
      break

    # Skip non-merges
    parents = repo.changelog.parents(mr)
    if parents[1] is nullrev:
      continue

    # Is this a marge from our topic branch? If so we're done.
    if parents[1] in topicSet:
      return parents[1]

  # Couldn't find anything.
  return None

#################################################################################
def calcBranchState(repo, branch, node):
  """ Figure out whether the given branch is local, or has been merged to
      dev/stage/prod. """

  # Detect if the branch has been closed
  states = {node: 'closed' if 'close' in repo.changelog.read(node)[5] else 'local'}

  # Now check for pushes to dev and stage. These are record in a special file
  # because they're not merges and thus not part of the normal repo history.
  #
  readTopicState(repo)
  for (target, hexnode) in topicState.get(branch, {}).iteritems():
    states[repo[hexnode].node()] = target

  # Also look for pushes to prod; these are actual merges.
  if branch != repo.topicProdBranch:
    topicRevs = list(revsOnBranch(repo, branch))
    found = findMainMerge(repo, topicRevs, repo.topicProdBranch)
    if found:
      states[found] = repo.topicProdBranch

  # Make the final textual version
  outList = []
  for (p, state) in states.iteritems():
    if p != node:
      state += "(%d)" % repo.changelog.rev(p)
    if p in repo.dirstate.parents():
      state = "*" + state
    outList.append(state)

  return ", ".join(outList)


#################################################################################
@command("topen|tnew",
          [('C', 'clean',  None, "discard uncommitted changes (no backup)"),
           ('P', 'nopull', None, "don't pull current data from master")],
          "newbranchname")
def topen(ui, repo, *args, **opts):
  """ open (create) a new topic branch """

  mustBeTopicRepo(repo)

  # Check the arguments
  if 'tmenu' in opts:
    resp = ui.prompt("Name for new branch:", None)
    if not resp:
      return 1
    args = [resp]

  elif len(args) < 1:
    ui.warn("Error: You must specify a name for the new branch.\n")
    return 1

  # Pull new changes from the central repo
  if not opts.get('nopull', False):
    if tryCommand(ui, "pull", lambda:commands.pull(ui, repo, **opts) >= 2):
      return 1

  # Validate the name
  target = args[0]
  if target in topicBranchNames(repo, closed=True) + [repo.topicProdBranch]:
    ui.warn("Error: a branch with that name already exists; try choosing a different name.\n")
    return 1

  # Make sure we're at the top of the prod branch
  if repo.dirstate.parents()[0] not in repo.branchheads('prod') or \
     repo.dirstate.branch() != repo.topicProdBranch:
    if tryCommand(ui, "update %s" % quoteBranch(repo.topicProdBranch), \
                  lambda:commands.update(ui, repo, node=repo.topicProdBranch, check=True)):
      return 1

  # Create the new branch and commit it.
  if tryCommand(ui, 'branch %s' % target, lambda:commands.branch(ui, repo, target)):
    return 1

  text = "Opening branch %s" % quoteBranch(target)
  return tryCommand(ui, "commit", lambda:repo.commit(text) is None)

#################################################################################
@command("tbranch|tb",
         [('C', 'clean', None, "discard uncommitted changes (no backup)")],
         "[branchname]")
def tbranch(ui, repo, *args, **opts):

  """ show current branch status or switch to a different branch """

  mustBeTopicRepo(repo)

  # If called from the menu, allow user to choose a branch
  if 'tmenu' in opts:
    branches = topicBranchNames(repo)
    if not branches:
      ui.warn("There are no branches to switch to.\n")
      return 1

    ui.status("Branches you can switch to:\n")
    for i in range(len(branches)):
      ui.status("  %4d. %s\n" % (i+1, branches[i]))

    resp = ui.prompt("Which one (1-%d): " % len(branches), '')
    found = None
    i = 0
    while i < len(branches):
      if str(i+1) == resp:
        found = branches[i]
        break
      i += 1
    if not found:
      ui.warn("Unknown number '%s'\n" % resp)
      return 1

    ui.status("Switching to branch: %s\n" % branches[i])
    args = [branches[i]]

  # If no branch specified, show the status of the current branch.
  if len(args) < 1:
    ui.status("Current branch: %s\n" % repo.dirstate.branch())
    ui.status("        Status: %s\n" % calcBranchState(repo, repo.dirstate.branch(), repo.dirstate.parents()[0]))
    return

  # Otherwise, switch to a different (existing) branch
  target = args[0]
  if target == repo.dirstate.branch():
    ui.status("You're already on that branch.\n")
    return

  if not target in topicBranchNames(repo, closed=True) + [repo.topicProdBranch]:
    maybes = topicBranchNames(repo) + [repo.topicProdBranch] # don't check closed branches for maybes
    matches = [b for b in maybes if target.lower() in b.lower()]
    if len(matches) != 1:
      ui.warn("Error: branch '%s' does not exist.\n" % target)
      return 1
    ui.status("(branch %s matches '%s')\n" % (matches[0], target))
    target = matches[0]

  return commands.update(ui, repo, 
                         node = target, 
                         check = not opts.get('clean', False),
                         clean = opts.get('clean', False))


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
def getBranchTags(repo):

  # Mercurial < 2.9
  if hasattr(repo, 'branchtags'):
    return repo.branchtags()

  # Mercurial >= 2.9
  def branchtip(repo, heads):
    tip = heads[-1]
    for h in reversed(heads):
      if not repo[h].closesbranch():
        tip = h
        break
    return tip
  result = {}
  for bn, heads in repo.branchmap().iteritems():
    result[bn] = branchtip(repo, heads)
  return result


#################################################################################
def topicBranches(repo, closed = False):
  """ Get a list of the topic branches, ordered by descending date of last change. 
      Each list entry is a tuple (branchname, headCtx) """

  # Helper function to produce the set of direct parent branches
  def parentBranches(ctx):

    # Deal with workingctx
    node = ctx.node() if ctx.node() is not None else ctx.parents()[0].node()

    # Init
    visit = [node]
    parentBranches = set()
    while len(visit):
      node = visit.pop()

      # If we haven't reached a parent branch yet, keep going
      br = repo.changelog.read(node)[5].get("branch")
      if br == ctx.branch():
        parents = repo.changelog.parents(node)
        visit.append(parents[0])
        if not parents[1] == nullrev:
          visit.append(parents[1])
      else:
        parentBranches.add(br)

    return parentBranches

  # Process each branch
  result = []
  for tag, node in getBranchTags(repo).items():

    # Skip prod
    if tag in repo.topicSpecialBranches:
      continue

    # Skip closed branches if requested
    ## The next line used to be `repo.lookup(node)`, but that stopped working in hg 4.6.
    ## Not sure why/whether it was ever needed.
    hn = node
    if not closed:
      if not hn in repo.branchheads(tag, closed=False):
        continue

    # Skip branches that don't originate from prod
    if not repo.topicProdBranch in parentBranches(repo[hn]):
      continue

    # Determine all the field values we're going to print
    result.append((tag, repo[hn]))

  result.sort(lambda a,b: (a[1].date() < b[1].date()) - (a[1].date() > b[1].date()))
  return result


#################################################################################
def topicBranchNames(repo, closed = False):
  """ Return a list of the open topic branches. """

  ret = [tup[0] for tup in topicBranches(repo, closed)]

  rb = repo.dirstate.branch()
  if rb not in ret and rb not in repo.topicSpecialBranches:
    ret.insert(0, repo.dirstate.branch())
  return ret


#################################################################################
@command("tbranches|tbs",
         [('c', 'closed', None, "include closed branches")],
         "")
def tbranches(ui, repo, *args, **kwargs):
  """ show recent activity on the currently open topic branches """

  mustBeTopicRepo(repo)

  closed = kwargs.get('closed', False)

  # Process each branch
  toPrint = []
  branches = set()
  for (branch, ctx) in topicBranches(repo, closed):

    # Determine all the field values we're going to print
    branches.add(branch)
    user = shortuser(ui, ctx.user())
    date = shortdate(ctx.date())
    branchState = calcBranchState(repo, branch, ctx.node())

    descrip = ctx.description().splitlines()[0].strip()
    if len(descrip) > 60:
      descrip = descrip[0:57] + "..."

    toPrint.append((branch, user, date, branchState, descrip))

  if repo.dirstate.branch() not in branches and \
     repo.dirstate.branch() not in repo.topicSpecialBranches + ['']:
    toPrint.insert(0, (repo.dirstate.branch(), 'n/a', 'n/a', '*local', '<not yet committed>'))

  printColumns(ui, ('Branch', 'Who', 'When', 'Where', 'Description'), toPrint)


#################################################################################
@command("tlog|tl",
         [('a', 'all', None, "all topic branches, not just current"),
          ('c', 'closed', None, "include closed branches")] \
          + commands.templateopts,
         "[branches]")
def tlog(ui, repo, *pats, **opts):
  """ show revision history of the current branch (or all branches) """

  mustBeTopicRepo(repo)

  # If called from menu, substitute appropriate options
  if 'tmenu' in opts:
    opts = {'all':False}

  if opts['all']:
    branches = topicBranchNames(repo, opts.get('closed', False))
  elif pats:
    branches = pats
  else:
    branches = [repo.dirstate.branch()]

  for branch in branches:

    str = "Branch: " + branch
    ui.status("\n%s\n" % str)
    ui.status("%s\n" % ("=" * len(str)))

    if not branch in repo.branchmap():
      if branch == repo.dirstate.branch():
        ui.status("<not yet committed>\n")
      else:
        ui.status("<unknown>\n")
      continue

    toPrint = []

    for id in repo:
      ctx = repo[id]
      kind = None
      if ctx.branch() == branch:
        kind = 'closed' if 'close' in ctx.changeset()[5] else 'local'
      elif ctx.parents() and ctx.parents()[0].branch() == branch:
        kind = ctx.branch()
      elif len(ctx.parents()) > 1 and ctx.parents()[1].branch() == branch:
        kind = ctx.branch()
      
      if kind and ctx in [repo[p] for p in repo.dirstate.parents()]:
        kind = "*" + kind

      if kind:
        user = shortuser(ui, ctx.user())
        date = shortdate(ctx.date())
        descrip = ctx.description().splitlines()[0].strip()
        if len(descrip) > 60: descrip = descrip[0:57] + "..."
        toPrint.append((ctx.rev(), user, date, kind, descrip))

    # To get same order as hg log
    toPrint.reverse()

    printColumns(ui, ('Rev num', 'Who', 'When', 'Where', 'Description'), toPrint, indent=4)


###############################################################################
def tryCommand(ui, descrip, commandFunc, showOutput = False, repo = None):
  """ Run a command and capture its output. Print the output only if it fails. """

  ui.lastTryCommandOutput = ""
  ui.status("  hg %s\n" % descrip)
  ui.flush()

  # For some reason push isn't responsive to ui.pushbuffer, so we have to work
  # harder to grab its output.
  #
  class Grabber(StringIO):
    def __init__(self, echoTo):
      StringIO.__init__(self)
      self.echoTo = echoTo
    def write(self, str):
      if self.echoTo:
        if "(branch merge, don't forget to commit)" not in str:
          self.echoTo.write("    " + str)
      StringIO.write(self, str)

  buffer = Grabber(sys.stdout if (ui.verbose or showOutput) else None)
  (oldStdout, oldStderr) = (sys.stdout, sys.stderr)
  (sys.stdout, sys.stderr) = (buffer, buffer)
  if hasattr(ui, 'fout'): # new for Mercurial 2.8+ (approx)
    oldFout = ui.fout
    ui.fout = buffer
    if repo and hasattr(repo, 'baseui'):
      oldBaseFout = repo.baseui.fout
      repo.baseui.fout = buffer
  if hasattr(ui, 'ferr'): # new for Mercurial 2.8+ (approx)
    oldFerr = ui.ferr
    ui.ferr = buffer
    if repo and hasattr(repo, 'baseui'):
      oldBaseFerr = repo.baseui.ferr
      repo.baseui.ferr = buffer

  # Now run the command
  res = 1
  try:

    res = commandFunc()

  finally:

    # Restore in/out streams
    (sys.stdout, sys.stderr) = (oldStdout, oldStderr)
    if hasattr(ui, 'fout'): # new for Mercurial 2.8+ (approx)
      ui.fout = oldFout
      if repo and hasattr(repo, 'baseui'):
        repo.baseui.fout = oldBaseFout
    if hasattr(ui, 'ferr'): # new for Mercurial 2.8+ (approx)
      ui.ferr = oldFerr
      if repo and hasattr(repo, 'baseui'):
        repo.baseui.ferr = oldBaseFerr

    # If we were suppressing output but there's been an error, show it.
    outFunc = ui.warn if (res and not ui.verbose and not showOutput) else lambda x: x
    outFunc("\n")
    for line in buffer.getvalue().split("\n"):
      outFunc("    " + line + "\n")

    # Record the output for reference
    ui.lastTryCommandOutput = buffer.getvalue()

  # All done; let the caller know how it went.
  return res


###############################################################################
def onTopicBranch(ui, repo):
  """ Check if the repo is currently on a topic branch. """

  branch = repo.dirstate.branch()
  if branch in repo.topicSpecialBranches:
    ui.warn("Error: you are not currently on a topic branch. Use 'tbranch' to switch to one.\n")
    return False
  return True


###############################################################################
def isClean(ui, repo):
  """ Check if the repo is clean (no uncommitted changes). If not, print a message. """

  # If stuff hasn't been committed yet it doesn't make sense to push.
  if repo[None].dirty():
    ui.warn("Error: There are uncommitted changes.\n");
    return False
  return True


###############################################################################
def quoteBranch(name):
  """ If the given name needs quoting for regular shell use, quote it now. """

  if re.search("[^-a-zA-Z0-9._]", name):
    return '"%s"' % name
  return name

###############################################################################
def doMerge(ui, repo, branch):
  """ Merge from the given branch into the working directory. """

  # Try the merge. Don't hide the output as merge sometimes needs to ask the user questions.
  if tryCommand(ui, "merge %s" % quoteBranch(branch), lambda:hg.merge(repo, revsymbol(repo, branch)), showOutput = True):
    res = ui.prompt("Merge failed! Do you want to fix it manually? (if not, will return to clean state):", default="y")
    if res.lower() != "y" and res.lower() != "yes":
      ui.status("Getting back to clean state...\n")
      tryCommand(ui, "update --clean", lambda:hg.clean(repo, repo.dirstate.branch()))
    else:
      ui.status("Ok, leaving directory in partly merged state. To finish:\n")
      ui.status("  $ hg resolve --list        # list conflicting files\n")
      ui.status("  ...fix fix fix...\n")
      ui.status("  $ hg resolve --mark --all  # shortcut: resolve -am\n")
      ui.status("  $ hg commit\n")
      ui.status("Or, to abandon the merge:\n")
      ui.status("  $ hg update --clean\n")
    return 1

  return 0

###############################################################################
@command("tfreshen|tf",
         [('P', 'nopull',    None, "don't pull current data from master"),
          ('C', 'nocommit',  None, "merge only, don't commit")] 
          + commands.remoteopts,
          "")
def tfreshen(ui, repo, *args, **opts):
  """ Pull recent changes from prod and merge them into the current branch. """

  mustBeTopicRepo(repo)

  # Sanity checks
  if not(onTopicBranch(ui, repo) and isClean(ui, repo)):
    return 1
  topicBranch = repo.dirstate.branch()

  commitStop = False # note if we get to that step

  # Pull new changes from the central repo
  if not opts.get('nopull', False):
    if tryCommand(ui, "pull", lambda:commands.pull(ui, repo, **opts) >= 2):
      return 1

  # Are there any changes to merge?
  workDir = repo[None].parents()[0]
  prodTip = repo[repo.branchheads(repo.topicProdBranch)[0]]
  ancestor = prodTip.ancestor(workDir)
  #print("prodTip:" + str(prodTip) + ", workDir:" + str(workDir) + ", ancestor:" + str(ancestor))
  if prodTip.node() == ancestor.node() or topicBranch in [p.branch() for p in prodTip.parents()]:
    ui.status("  (%s is still fresh)\n" % topicBranch)
    return 0
  else:
    ui.status("  (will freshen %s from %s)\n" % (topicBranch, repo.topicProdBranch))

  # Merge it.
  if doMerge(ui, repo, repo.topicProdBranch):
    return 1

  # Stop if requested.
  if opts.get('nocommit', False):
    ui.status("\nStopping before commit as requested.\n")
    commitStop = True
    return 0

  # Unlike a normal hg commit, if no text is specified we supply a reasonable default.
  text = opts.get('message')
  if text is None:
    text = "Merge recent changes from %s" % repo.topicProdBranch

  # Commit the merge.
  if tryCommand(ui, "commit", lambda:repo.commit(text) is None):
    return 1

  if not opts.get('terse', False):
    ui.status("Done.\n")

###############################################################################
def writeTopicState(repo):
  """ Write the current state of the topic branches. """

  filePath = os.path.join(repo.path, 'topicstate')
  with open(filePath, "w") as f:
    f.write(repr(topicState))

###############################################################################
def readTopicState(repo):
  """ Read in the stored state of the topic branches. """
  
  filePath = os.path.join(repo.path, 'topicstate')
  if os.path.exists(filePath):
    with open(filePath, "r") as f:
      global topicState
      topicState = eval(f.read(), {}, {})

###############################################################################
@command("tpush|tp",
         [('m', 'message',   None, "use <text> as commit message instead of default 'Merge to <branch>'"),
          ('P', 'nopull',    None, "don't pull current data from master"),
          ('C', 'nocommit',  None, "merge only, don't commit or push")] 
          + commands.remoteopts,
         "dev|stage|prod")
def tpush(ui, repo, *args, **opts):
  """ Push current branch to dev, stage, or production (and merge if prod). """

  mustBeTopicRepo(repo)

  # Sanity checks
  if not(onTopicBranch(ui, repo) and isClean(ui, repo)):
    return 1
  topicBranch = repo.dirstate.branch()

  # If called from the menu, allow user to choose target branch
  if 'tmenu' in opts:
    resp = ui.prompt("Push to which machine(es) [dsp], or blank to just share:", "")

    for c in resp:
      if c.upper() not in ('D', 'S', 'P'):
        ui.warn("Unknown push target '%s'\n" % resp)
        return 1

    args = []
    resp = resp.upper()
    if 'D' in resp: args.append("dev")
    if 'S' in resp: args.append("stage")
    if 'P' in resp: args.append(repo.topicProdBranch)

    opts = { 'nopull':False, 'nocommit':False, 'message':None }

  # If pushing to prod, get extra verification from the user
  nameSet = set()
  for name, path in ui.configitems("paths"):
    nameSet.add(name)
  if repo.topicProdBranch in args:
    if not repo.topicProdBranch in nameSet:
      raise util.Abort("Error: '%s' server path not configured in .hg/hgrc." % repo.topicProdBranch)
    res = ui.prompt("This will push to production. Okay to proceed?")
    if res.lower() != "y" and res.lower() != "yes":
      raise util.Abort("Ok.")
    if "stage" in nameSet and not "stage" in args:
      args = ["stage"] + list(args)
    if "dev" in nameSet and not "dev" in args:
      args = ["dev"] + list(args)

  # We'll use a special set of options for hg push commands
  pushOpts = copy.deepcopy(opts)
  pushOpts['new_branch'] = True  # allow pushing new branches

  # If no branch was specified, just share the current branch
  if len(args) < 1:
    if tryCommand(ui, "push --new-branch -b %s" % quoteBranch(topicBranch),
                  lambda:commands.push(ui, repo, branch=(topicBranch,), **pushOpts),
                  repo=repo):
      return 1
    ui.status("Done.\n")
    return 0

  commitStop = False # note if we get to that step

  # If pushing to prod, merge changes from this branch into the prod branch.
  alreadyMerged = [p.branch() for p in repo[None].parents()[0].children()]
  if repo.topicProdBranch in args and not repo.topicProdBranch in alreadyMerged:

    # Freshen with recent changes from prod
    freshenOpts = copy.deepcopy(opts)
    freshenOpts['terse'] = True # avoid printing "Done." at end
    if tfreshen(ui, repo, *args, **freshenOpts):
      return 1

    # Update to the prod
    if tryCommand(ui, "update %s" % quoteBranch(repo.topicProdBranch),
                  lambda:hg.update(repo, revsymbol(repo, repo.topicProdBranch))):
      return 1

    # Merge from the topic branch
    if doMerge(ui, repo, topicBranch):
      return 1

    # Stop if requested.
    if opts['nocommit']:
      ui.status("\nStopping before commit as requested.\n")
      commitStop = True
      return 0

    # Unlike a normal hg commit, if no text is specified we supply a reasonable default.
    text = opts.get('message')
    if text is None:
      text = "Merge %s to %s" % (topicBranch, repo.topicProdBranch)

    # Commit the merge.
    if tryCommand(ui, "commit", lambda:repo.commit(text) is None):
      return 1

    # And return to the original topic branch
    if repo.dirstate.branch() != topicBranch:
      if tryCommand(ui, "update %s" % quoteBranch(topicBranch), lambda:hg.update(repo, revsymbol(repo, topicBranch))):
        return 1

  # For prod, push to the central repo as well as the servers.
  if repo.topicProdBranch in args:
    try:
      if tryCommand(ui, "push --new-branch -b %s default" % repo.topicProdBranch,
                    lambda:commands.push(ui, repo, branch=(repo.topicProdBranch,), **pushOpts),
                    repo=repo) > 1:
        return 1
    except Exception, e:
      if "Not allowed to create multiple heads in the same branch" in ui.lastTryCommandOutput \
         or "push creates new remote head" in ui.lastTryCommandOutput \
         or "push creates new remote head" in str(e):
        print("Attempting to repair multiple heads.")
        if repo.dirstate.branch() != topicBranch:
          if tryCommand(ui, "update %s" % quoteBranch(topicBranch), lambda:hg.update(repo, revsymbol(repo, topicBranch))) > 1:
            return 1
        topicDir = os.path.dirname(__file__)
        repairScript = os.path.join(topicDir, "repair.py")
        subprocess.check_call([repairScript, repo.topicProdBranch, 'default'])
      else:
        raise

  # Now push to each server
  branchToPush = repo.topicProdBranch if repo.topicProdBranch in args else topicBranch
  for mergeTo in args:

    # Cannot merge to default, or to a topic branch
    if mergeTo in repo.topicIgnoreBranches and mergeTo not in ('dev', 'stage'):
      ui.warn("Cannot merge to '%s' branch.\n" % mergeTo)
      return 1

    # Push to the correct server
    if mergeTo in nameSet:
      if tryCommand(ui, "push --new-branch -b %s %s" % (branchToPush, mergeTo),
                    lambda:commands.push(ui, repo, branch=(branchToPush,), dest=mergeTo, **pushOpts),
                    repo=repo) > 1:   # 1 means nothing to push, which is ok
        return 1
    else:
      print("Note: No path for server '%s' is configured in .hg/hgrc." % mergeTo)

    # Record the push so we can display the right branch status later
    readTopicState(repo)
    if not topicBranch in topicState:
      topicState[topicBranch] = {}
    topicState[topicBranch][mergeTo] = repo[repo.dirstate.parents()[0]].hex()
    writeTopicState(repo)

  ui.status("Done.\n")

###############################################################################
@command("tclose",
         [('m', 'message',   None, "use <text> as commit message instead of default 'Closing <branch>'"),
          ('P', 'nopull',    None, "don't pull current data from master"),
          ('',  'nopush',    None, "don't push result to master")]
         + commands.remoteopts,
         "[branches]")
def tclose(ui, repo, *args, **opts):
  """ close the current topic branch and push to the central repository """

  mustBeTopicRepo(repo)

  # Sanity check
  if not isClean(ui, repo):
    return 1

  if args:
    branches = args
  else:
    if not onTopicBranch(ui, repo):
      return 1
    branches = [repo.dirstate.branch()]

  if 'tmenu' in opts:
    if ui.prompt("Branch '%s': close it?" % branches[0]).upper() != 'Y':
      return 1
    opts = { 'nopull':False, 'nopush':False }

  pulled = False # only pull once

  for branch in branches:

    # Pull new changes from the central repo to avoid multiple-heads problem
    if not opts['nopull'] and not pulled:
      if tryCommand(ui, "pull", lambda:commands.pull(ui, repo, **opts) >= 2):
        return 1
      pulled = True

    # Can't close already closed branches, nor any of the special branches
    if not repo.branchheads(branch) or branch in repo.topicSpecialBranches:
      ui.warn("Error: %s is not an open topic branch\n" % branch)
      return 1

    # Now update to the head of the branch being closed
    if repo.dirstate.parents()[0] not in repo.branchheads(branch):
      if tryCommand(ui, "update %s" % quoteBranch(branch), lambda:commands.update(ui, repo, node=branch)):
        return 1

    # Unlike a normal hg commit, if no text is specified we supply a reasonable default.
    branch = repo.dirstate.branch()
    text = opts.get('message')
    if text is None:
      text = "Closing %s" % branch

    # Close it
    if tryCommand(ui, "commit --close-branch", lambda:repo.commit(text, extra = {'close':'True'}) is None):
      return 1

    # Aditionally, for this to not be considered a "head" it has to have a
    # child commit. So we have to merge into prod. First, update.
    #
    if tryCommand(ui, "update %s" % repo.topicProdBranch, lambda:commands.update(ui, repo, node=repo.topicProdBranch)):
      return 1

    # Now merge, ignoring all conflicts.
    mergeOpts = copy.deepcopy(opts)
    mergeOpts['tool'] = "internal:fail"
    mergeOpts['noninteractive'] = True
    # Ignore return value... ok if merge fails
    tryCommand(ui, "merge -r %s" % quoteBranch(branch), 
               lambda:commands.merge(ui, repo, node=branch, **mergeOpts),
               repo = repo)

    # Revert all files to prod (regardless of what happened on the branch)
    revertOpts = copy.deepcopy(opts)
    revertOpts['all'] = True
    revertOpts['rev'] = "."
    if tryCommand(ui, "revert -a -r .", lambda:commands.revert(ui, repo, **revertOpts), repo = repo):
      return 1

    # Were there any merge conflicts?
    resolveOpts = copy.deepcopy(opts)
    resolveOpts['list'] = True
    if tryCommand(ui, "resolve -l", lambda:commands.resolve(ui, repo, **resolveOpts), repo = repo):
      return 1

    # Anything that had a merge conflict, mark it resolved (by the revert)
    if ui.lastTryCommandOutput != '':
      resolveOpts = copy.deepcopy(opts)
      resolveOpts['all'] = True
      resolveOpts['mark'] = True
      if tryCommand(ui, "resolve -a -m", lambda:commands.resolve(ui, repo, **resolveOpts), repo = repo):
        return 1

    # Commit the merge
    if tryCommand(ui, "commit", lambda:repo.commit(text) is None):
      return 1

  # And push.
  if not opts['nopush']:
    pushOpts = copy.deepcopy(opts)
    if 'message' in pushOpts:
      del pushOpts['message']
    pushOpts['force'] = True
    nameSet = set()
    for name, path in ui.configitems("paths"):
      nameSet.add(name)
    if tryCommand(ui, "push -f -b %s -b %s default" % (quoteBranch(branch), repo.topicProdBranch), 
                  lambda:commands.push(ui, repo, branch=(branch,repo.topicProdBranch), **pushOpts), 
                  repo=repo) > 1:
      return 1
    if "dev" in nameSet:
      if tryCommand(ui, "push -f -b %s -b %s dev" % (quoteBranch(branch), repo.topicProdBranch), 
                    lambda:commands.push(ui, repo, branch=(branch,repo.topicProdBranch), dest="dev", **pushOpts), 
                    repo=repo) > 1:
        return 1
    if "stage" in nameSet:
      if tryCommand(ui, "push -f -b %s -b %s stage" % (quoteBranch(branch), repo.topicProdBranch), 
                    lambda:commands.push(ui, repo, branch=(branch,repo.topicProdBranch), dest="stage", **pushOpts), 
                    repo=repo) > 1:
        return 1
    if "prod" in nameSet:
      if tryCommand(ui, "push -f -b %s -b %s prod" % (quoteBranch(branch), repo.topicProdBranch), 
                    lambda:commands.push(ui, repo, branch=(branch,repo.topicProdBranch), dest="prod", **pushOpts), 
                    repo=repo) > 1:
        return 1

  ui.status("Done.\n")

###############################################################################
@command("tsync", [], "")
def tsync(ui, repo, *args, **opts):
  """ synchronize (pull & update) the current repo; also the topic extension itself. """

  mustBeTopicRepo(repo)

  # Pull and update the current repo
  pullOpts = copy.deepcopy(opts)
  pullOpts['update'] = True
  if tryCommand(ui, "pull -u", lambda:commands.pull(ui, repo, **pullOpts) >= 2):
    return 1

  # Then pull and update the topic extension
  topicDir = os.path.dirname(__file__)
  timeBefore = os.path.getmtime(os.path.join(topicDir, "topic.py"))
  if tryCommand(ui, "pull -R %s -u" % quoteBranch(topicDir), lambda:os.system('hg pull -R "%s" --quiet -u' % topicDir)):
    return 1
  timeAfter = os.path.getmtime(os.path.join(topicDir, "topic.py"))

  if timeBefore != timeAfter:
    ui.status("Note: Topic extension has been updated.\n")
    if 'tmenu' in opts:
      ui.status("...restarting menu.\n")
      os.system("hg tmenu")
      sys.exit(0)


###############################################################################
@command("tmenu", [], "")
def tmenu(ui, repo, *args, **opts):
  """ menu-driven interface to the 'topic' extension """

  global waitingForInput

  mustBeTopicRepo(repo)

  menuEntries = [('o', topen,     "open new branch"),
                 ('l', tlog,      "log (history)"),
                 ('f', tfreshen,  "freshen recent changes"),
                 ('p', tpush,     "push to dev/stage/prod"),
                 ('c', tclose,    "close branch"),
                 ('s', tbranches, "show open branches"),
                 ('b', tbranch,   "branch switch"),
                 ('y', tsync,     "sync (pull & upd)"),
                 ('q', None,      "quit menu")]
  cols = ([], [])
  colSizes = [0, 0]
  colNum = 0
  div = (len(menuEntries)+1) / 2
  for i in range(len(menuEntries)):
    ent = menuEntries[i]
    str = "[%c] %s" % (ent[0], ent[2])
    if i == div:
      colNum += 1
    cols[colNum].append(str)
    colSizes[colNum] = max(colSizes[colNum], len(str))
  cols[colNum].append("") # in case of odd number

  # Loop until user quits
  printFull = (len(args) == 0)
  while True:

    # Print the whole menu the first time, or after an unknown choice
    if printFull:
      ui.status("\n")
      tbranch(ui, repo)
      ui.status("\nTopic branch menu (v%s):\n\n" % topicVersion)
      for i in range(len(cols[0])):
        ui.status("    %-*s    %-*s\n" % (colSizes[0], cols[0][i], colSizes[1], cols[1][i]))
      printFull = False

    # Now get the user's choice
    if len(args):
      args = list(args)
      resp = args.pop(0)
    else:
      allChoices = "".join([ent[0] for ent in menuEntries])
      repoTimeBefore = os.path.getmtime(repo.path)
      resp = ui.prompt("\nCommand [%s or ?]:" % allChoices, default='')
      repoTimeAfter = os.path.getmtime(repo.path)
      if repoTimeBefore != repoTimeAfter:
        ui.warn("  (refreshing changed repository)\n")
        sys.argv = [sys.argv[0], 'tmenu', resp]
        dispatch.run() # should never return
        resp = "q"     # ...but just in case it does return, don't do anything

    # Handle quit separately
    if resp.upper() == 'Q':
      quit = True
      break

    # Handle '?'
    if resp == '?' or resp == '':
      printFull = True
      continue

    # Execute the corresponding command
    found = [ent for ent in menuEntries if resp.upper() == ent[0].upper()]
    if found:
      # Add an option so the command can prompt for additional info if necessary
      ui.status(found[0][2] + "\n")
      try:
        found[0][1](ui, repo, tmenu = True)
      except Exception, e:
        try:
          while True: ui.popbuffer()
        except Exception:
          pass
        ui.warn("Error: ")
        ui.warn("%r" % e)
        ui.warn("\n")
    else:
      ui.status("Unknown option: '%s'\n" % resp)
      printFull = True


###############################################################################
@command("tversion", [], "")
def tversion(ui, repo, *args, **opts):
  """ print out the version of the topic extension """

  ui.status("Topic extension version: %s\n" % topicVersion)


#################################################################################
@command("tsetup", [], "")
def tsetup(ui, repo, *args, **kwargs):
  """ Used to set up the topic extension in a pre-configured directory.
      We look for a prod branch in the current directory, and try to find a 
      .topic_hgrc file there.
  """

  mustBeTopicRepo(repo)

  ui.status("Topic setup:\n")
  
  # Make sure we're at the top of the prod branch
  if repo.dirstate.parents()[0] not in repo.branchheads('prod') or \
     repo.dirstate.branch() != repo.topicProdBranch:
    if tryCommand(ui, "update %s" % quoteBranch(repo.topicProdBranch), \
                  lambda:commands.update(ui, repo, node=repo.topicProdBranch, check=True)):
      return 1

  # See if there's a .topic_hgrc_adds file.
  addsFile = os.path.join(repo.path, "..", ".topic_hgrc_adds")
  if not os.path.exists(addsFile):
    raise util.Abort("There is no .topic_hgrc_adds file in your repository. Without that,\n" +
                     "the Topic extension cannot guess how to configure itself.")
  with open(addsFile, "r") as f:
    toAdd = f.read()

  # Stick in the extension
  topicDir = os.path.dirname(__file__)
  toAdd += "\n[extensions]\ntopic = %s\n" % os.path.join(topicDir, "topic.py")

  # Offer to add things to the .hgrc file
  ui.status("The following will be added to the .hg/hgrc file for this repo:\n\n" + toAdd)
  res = ui.prompt("\nOkay to proceed?", default="y")
  if res.lower() != "y" and res.lower() != "yes":
    raise util.Abort("Ok.")

  # Let's do it.
  hgrcFile = os.path.join(repo.path, "hgrc")
  with open(hgrcFile, "r") as f:
    existing = f.read()

  with open(hgrcFile, "w") as f:
    f.write(existing + "\n" + toAdd)

  ui.status("Done.\n")


#################################################################################
def checkPush(orig, ui, repo, *args, **kwargs):
  """ If pushing from a topic branch to dev or stage, record the push in the 
      topicstate file so that tbranches will show the right status. This is
      needed because these pushes don't have merges and therefore are not
      part of the normal repository record. """

  status = orig(ui, repo, *args, **kwargs)

  target = 'dev' if 'dev' in args \
           else 'stage' if 'stage' in args \
           else None
  if target and isTopicRepo(repo) and status == 0:
    readTopicState(repo)
    if 'branch' in kwargs and len(kwargs['branch']) > 0:
      topicBranch = kwargs['branch'][0]
    else:
      topicBranch = repo.dirstate.branch()
    if not topicBranch in topicState:
      topicState[topicBranch] = {}
    topicState[topicBranch][target] = repo[repo.dirstate.parents()[0]].hex()
    writeTopicState(repo)
  return status

#################################################################################
def reposetup(ui, repo):
  """ Set up branch names we need. These are typically 'dev', 'stage', and 'prod'
      but they can be overridden in a config file.
  """

  repo.topicAncestorCache = {}
  repo.topicProdBranch = ui.config('topic', 'prod-branch', default = 'prod')
  repo.topicIgnoreBranches = re.split("[ ,]+", ui.config('topic', 'ignore-branches', default = 'default'))
  repo.topicSpecialBranches = [repo.topicProdBranch] + repo.topicIgnoreBranches


#################################################################################
def uisetup(ui):
  """ Hook into the standard push command, to record topic branch pushes """

  extensions.wrapcommand(commands.table, 'push', checkPush)


#################################################################################
# Watcher for asynchronous pushes. Start from the command line. Will run forever
# unless something goes wrong; is intended to be started every 10 minutes by a
# cron job.
#
if __name__ == '__main__':

  lockFile = None

  try:
    import fcntl
  except ImportError:
    sys.stderr.write("fcntl not available, cannot lock watcher.")
    sys.exit(1)

  try:
    lockFile = open(os.path.join(os.getenv("HOME"), ".topic_async_lock"), "w+")

    try:
      fcntl.flock(lockFile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError, e:
      print("(not starting duplicate watcher)")
      sys.exit(0)

    lockFile.seek(0)
    lockFile.write("1")
    lockFile.flush()

    tmpDir = os.path.join(os.getenv("HOME"), ".topic_asyncPushes")
    print("Waiting for asynchronous pushes to appear in '%s'." % tmpDir)
    startTime = time.time()

    procs = []

    # Loop forever
    while True:

      # See if any work has appeared for us to do
      for tmpName in os.listdir(tmpDir):
        tmpPath = os.path.join(tmpDir, tmpName)
        with open(tmpPath, "r") as tmpFile:
          line = tmpFile.readline().strip()
          print("%s: Waiting 5 sec before async command '%s'." % (datetime.datetime.now().isoformat(), line))
          sys.stdout.flush()
          time.sleep(5)
          print("%s: Starting command '%s'." % (datetime.datetime.now().isoformat(), line))
          sys.stdout.flush()
          proc = subprocess.Popen(line, shell=True)
          proc.cmdLine = line
          procs.append(proc)
        os.remove(tmpPath)

      # Clean up any sub-processes that are done.
      newProcs = []
      for proc in procs:
        proc.poll()
        if proc.returncode is None:
          newProcs.append(proc)
        else:
          print("%s: Command '%s' returned %d." % (datetime.datetime.now().isoformat(), proc.cmdLine, proc.returncode))
          sys.stdout.flush()
      procs = newProcs

      # Be sure our log file gets updated
      sys.stdout.flush()

      # Wait for more work
      time.sleep(2)

  # And be sure to unlock the control lock file
  finally:
    if lockFile is not None:
      lockFile.seek(0)
      lockFile.write("0")
      fcntl.flock(lockFile, fcntl.LOCK_UN)
      lockFile.close()

