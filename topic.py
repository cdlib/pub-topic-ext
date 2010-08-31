#!/usr/bin/env python

'''topic

Extensions for eScholarship-style topic branches.
'''

import copy, heapq, os, re, sys, traceback
from xml import sax
from StringIO import StringIO
from mercurial import cmdutil, commands, context, dispatch, extensions, context, hg, patch, url, util
from mercurial.node import nullid, nullrev

global origCalcChangectxAncestor

topicVersion = "1.5.7"


#################################################################################
def ruleError(ui, message):
  """ Convenience function: Output message through ui.warn(), and return False. """

  ui.warn("Error: %s\n" % message)
  return False


#################################################################################
def isTopicRepo(repo):
  """ Check if the given repo is likely a topic repo. """

  bm = repo.branchmap()
  return repo.topicDevBranch in bm and repo.topicStageBranch in bm and repo.topicProdBranch in bm


#################################################################################
def mustBeTopicRepo(repo):
  """ Abort if the given repo isn't a topic repo. """

  if not isTopicRepo(repo):
    raise util.Abort("This doesn't appear to be a topic repository.")


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
  status = repo.status(ctx.parents()[0].node(), ctx.node())
  changedOrAdded = status[0] + status[1]
  for filename in changedOrAdded:
    data = ctx[filename].data()
    if util.binary(data):
      continue
    if data.startswith("<?xml "):
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

  # Don't allow dev/stage/prod branches to be closed.
  if ctx.extra().get('close') and thisBranch in repo.topicSpecialBranches:
    return ruleError(ui, "Not allowed to close the main dev/stage/prod branches.")

  # Prevent multiple heads on the same branch
  if len(repo.branchmap()[thisBranch]) > 1:
    return ruleError(ui, "Not allowed to create multiple heads in the same branch. Did you forget to merge?")

  # New branches may only branch from prod (old-style check)
  if thisBranch != p1Branch and p1Branch != repo.topicProdBranch:

    # Previously we allowed branching from dev; no longer.
    if util.shortdate(ctx.date()) >= "2010-07-07":
      return ruleError(ui, "Topics are only allowed to branch from '%s' directly." % repo.topicProdBranch)

  # (new-style check)
  if thisBranch not in (p1Branch, p2Branch) and (p1Branch, p2Branch) != (repo.topicProdBranch, None):

    # Previously we allowed a crazy case of merging two named branches to a third name; no longer
    if util.shortdate(ctx.date()) >= "2010-09-01":
      return ruleError(ui, "Topics are only allowed to branch from '%s' directly." % repo.topicProdBranch)

  isMerge = parent1 and parent2
  if isMerge:

    # Merges cannot go into default
    if thisBranch in repo.topicIgnoreBranches:
      return ruleError(ui, "Merging into default branch not allowed.")

    # Merges must come from topic branches
    if p2Branch in repo.topicSpecialBranches:
      return ruleError(ui, "Merge must come from a topic branch.")

    # Merge to stage must have gone to dev first; prod must have gone to stage.
    reqPred = repo.topicDevBranch if thisBranch == repo.topicStageBranch else repo.topicStageBranch if thisBranch == repo.topicProdBranch else None
    if reqPred:

      # Determine the last rev of this feature that was merged to the required predecessor.
      # For instance, if we're trying to merge to stage, find the last rev that was merged
      # to dev.
      #
      branchMap = repo.branchmap()
      assert reqPred in branchMap
      reqHead = branchMap[reqPred][0]
      ancestor = repo[reqHead].ancestor(parent2)

      #print "ctx:", str(ctx), int(ctx)
      #print "parent1:", str(parent1), int(parent2)
      #print "reqHead:", str(repo[reqHead]), int(repo[reqHead])
      #print "ancestor:", str(ancestor), int(ancestor)

      # If there have been changes to the feature since the last merge to the predecessor,
      # that means somebody forgot to do the missing merge.
      #
      if ancestor != parent2:
        return ruleError(ui, "You must merge changes to %s before %s." % (reqPred, thisBranch))

  else:

    # Non-merge. These plain commits are not allowed on dev/stage/prod.
    if thisBranch in repo.topicSpecialBranches:
      return ruleError(ui, "Direct commits to %s/%s/%s not allowed. Try making a topic branch." % 
                           (repo.topicDevBranch, repo.topicStageBranch, repo.topicProdBranch))

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
  for ctx in [repo[n] for n in range(int(repo[opts['node']]), len(repo))]:
    if "NO_TOPIC_VALIDATE" in ctx.description():
      return 0 # no problems

  # Check each incoming commit to make sure it meets our rules
  for ctx in [repo[n] for n in range(int(repo[opts['node']]), len(repo))]:
    #print int(ctx), ctx.branch(), ctx.description()
    parents = ctx.parents()
    if len(parents) == 1:
      parents.append(None)
    res = validateCommit(ui, repo, str(ctx), parents[0], parents[1])
    if res:
      return res

  # No problems found.
  return 0


#################################################################################
def repushChangegroup(ui, repo, hooktype, **opts):

  """ changegroup hook: if pushing to dev/stage/prod branch, push the stuff
      further on to the appropriate server. """

  # Silently skip this for non-topic repos
  if not isTopicRepo(repo):
    return 0

  # Figure out all the branches that were affected by the changesets.
  branchesChanged = set()
  for ctx in [repo[n] for n in range(int(repo[opts['node']]), len(repo))]:
    branchesChanged.add(ctx.branch())

  # Now check if any "repush-X" config entries are present, where X is
  # a matching branch name.
  #
  for branch in sorted(branchesChanged):
    repushTarget = ui.config('topic', 'repush-'+branch)
    if repushTarget:
      ui.status("Re-push %s branch: " % branch)
      commands.push(ui, repo, dest=repushTarget, force = True)
      ui.status("Done with re-push to %s\n" % branch)


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
  for ctx in [repo[n] for n in range(int(repo[opts['node']]), len(repo))]:
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

  # Check for any tabs being added in any file. They're not allowd.
  # MH: This is too stringent for now, need to approve this change with everybody.
  #if not checkTabs(ui, repo, node):
  #  return True # abort

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

  # Now check for merges to dev, stage, and prod
  topicRevs = list(revsOnBranch(repo, branch))
  for mainBr in (repo.topicDevBranch, repo.topicStageBranch, repo.topicProdBranch):
    found = findMainMerge(repo, topicRevs, mainBr)
    if found:
      states[found] = mainBr

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
def topen(ui, repo, *args, **opts):
  """ open (create) a new topic branch """

  mustBeTopicRepo(repo)

  if 'tmenu' in opts:
    resp = ui.prompt("Name for new branch:", None)
    if not resp:
      return 1
    args = [resp]

  elif len(args) < 1:
    ui.warn("Error: You must specify a name for the new branch.\n")
    return 1

  target = args[0]
  if target in topicBranchNames(repo, closed=True) + [repo.topicDevBranch, repo.topicProdBranch, repo.topicStageBranch]:
    ui.warn("Error: a branch with that name already exists; try choosing a different name.\n")
    return 1

  if repo.dirstate.branch() != repo.topicProdBranch:
    ui.status("Branching from %s...\n" % repo.topicProdBranch)
    res = commands.update(ui, repo, node=repo.topicProdBranch, check=True)
    if res: return res

  return commands.branch(ui, repo, target)


#################################################################################
def tbranch(ui, repo, *args, **opts):

  """ show current branch status or switch to a different branch """

  mustBeTopicRepo(repo)

  # If called from the menu, allow user to choose a branch
  if 'tmenu' in opts:
    branches = topicBranchNames(repo)
    if not branches:
      ui.warn("There are no branches to switch to.\n")
      return 1
    branches = sorted(branches)

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

  if not target in topicBranchNames(repo, closed=True) + [repo.topicDevBranch, repo.topicProdBranch, repo.topicStageBranch]:
    maybes = topicBranchNames(repo) + [repo.topicDevBranch, repo.topicProdBranch, repo.topicStageBranch] # don't check closed branches for maybes
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
  for tag, node in repo.branchtags().items():

    # Skip dev/stage/prod
    if tag in repo.topicSpecialBranches:
      continue

    # Skip closed branches if requested
    hn = repo.lookup(node)
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
    user = util.shortuser(ctx.user())
    date = util.shortdate(ctx.date())
    branchState = calcBranchState(repo, branch, ctx.node())

    descrip = ctx.description().splitlines()[0].strip()
    if len(descrip) > 60:
      descrip = descrip[0:57] + "..."

    toPrint.append((branch, user, date, branchState, descrip))

  if repo.dirstate.branch() not in branches and \
     repo.dirstate.branch() not in repo.topicSpecialBranches + ['']:
    toPrint.insert(0, (repo.dirstate.branch(), 'n/a', 'n/a', '*local', '<working directory>'))

  printColumns(ui, ('Branch', 'Who', 'When', 'Where', 'Description'), toPrint)


#################################################################################
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
        user = util.shortuser(ctx.user())
        date = util.shortdate(ctx.date())
        descrip = ctx.description().splitlines()[0].strip()
        if len(descrip) > 60: descrip = descrip[0:57] + "..."
        toPrint.append((int(ctx), user, date, kind, descrip))

    printColumns(ui, ('Rev num', 'Who', 'When', 'Where', 'Description'), toPrint, indent=4)


###############################################################################
def tryCommand(ui, descrip, commandFunc, showOutput = False):
  """ Run a command and capture its output. Print the output only if it fails. """

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
        self.echoTo.write("    " + str)
      StringIO.write(self, str)

  buffer = Grabber(sys.stdout if (ui.verbose or showOutput) else None)
  (oldStdout, oldStderr) = (sys.stdout, sys.stderr)
  (sys.stdout, sys.stderr) = (buffer, buffer)

  # Now run the command
  res = 1
  try:

    res = commandFunc()

  finally:

    # Restore in/out streams
    (sys.stdout, sys.stderr) = (oldStdout, oldStderr)
    outFunc = ui.warn if (res and not ui.verbose and not showOutput) else lambda x: x
    outFunc("\n")
    for line in buffer.getvalue().split("\n"):
      outFunc("    " + line + "\n")

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
def tpush(ui, repo, *args, **opts):
  """ merge current branch to dev, stage, or production and push it there """

  mustBeTopicRepo(repo)

  # Sanity checks
  if not(onTopicBranch(ui, repo) and isClean(ui, repo)):
    return 1
  mergeFrom = repo.dirstate.branch()

  # If called from the menu, allow user to choose target branch
  if 'tmenu' in opts:
    resp = ui.prompt("Which branch(es) [dsp]:", "")

    for c in resp:
      if c.upper() not in ('D', 'S', 'P'):
        ui.warn("Unknown push target '%s'" % resp)
        return 1

    resp = resp.upper()
    args = []
    if 'D' in resp: args.append(repo.topicDevBranch)
    if 'S' in resp: args.append(repo.topicStageBranch)
    if 'P' in resp: args.append(repo.topicProdBranch)

    opts = { 'nopull':False, 'nocommit':False, 'message':None }

  # Make sure a branch to push to was specified
  if len(args) < 1:
    ui.warn("Error: You must specify at least one branch (%s/%s/%s) to push to.\n" %
            (repo.topicDevBranch, repo.topicStageBranch, repo.topicProdBranch))
    return 1

  # Figure out where it's currently pushed to
  pushedTo = [p.branch() for p in repo.parents()[0].children()]

  # Make sure the arguments are in order (dev -> stage -> prod)
  tmp = set(pushedTo)
  alreadyMerged = set()
  for arg in args:
    need = {repo.topicDevBranch:None, repo.topicStageBranch:repo.topicDevBranch, repo.topicProdBranch:repo.topicStageBranch}.get(arg, 'bad')
    if need == 'bad':
      ui.warn("Error: you may only push to '%s', '%s', or '%s'.\n" % (repo.topicDevBranch, repo.topicStageBranch, repo.topicProdBranch))
      return 1
    elif arg in tmp:
      alreadyMerged.add(arg) # don't merge again
    elif need and need not in tmp:
      ui.warn("Error: you must push to %s before %s\n" % (need, arg))
      return 1
    tmp.add(arg)

  pulled = False # only pull once
  commitStop = False # note if we get to that step

  # Okay, let's go for it.
  try:

    # Pull new changes from the central repo
    if not opts['nopull'] and not pulled and set(args) - alreadyMerged:
      if tryCommand(ui, "pull", lambda:commands.pull(ui, repo, **opts)):
        return 1
      pulled = True

    for mergeTo in args:

      # Merge if necessary
      if not mergeTo in alreadyMerged:

        # Update to the branch we're going to merge into
        if tryCommand(ui, "update %s" % quoteBranch(mergeTo), lambda:hg.update(repo, mergeTo)):
          return 1

        # Merge it. Don't hide the output as merge sometimes needs to ask the user questions.
        if tryCommand(ui, "merge %s" % quoteBranch(mergeFrom), lambda:hg.merge(repo, mergeFrom), showOutput = True):
          ui.status("Merge failed! Getting back to clean state...\n")
          tryCommand(ui, "update -C %s" % quoteBranch(mergeFrom), lambda:hg.clean(repo, mergeFrom))
          return 1

        # Stop if requested.
        if opts['nocommit']:
          ui.status("\nStopping before commit as requested.\n")
          commitStop = True
          return 0

        # Unlike a normal hg commit, if no text is specified we supply a reasonable default.
        text = opts.get('message')
        if text is None:
          text = "Merge %s to %s" % (mergeFrom, mergeTo)

        # Commit the merge.
        if tryCommand(ui, "commit", lambda:repo.commit(text) is None):
          return 1

    # Push to the central server
    pushOpts = copy.deepcopy(opts)
    pushOpts['force'] = True # we very often create new branches and it's okay!
    if tryCommand(ui, "push -f -b %s" % (" -b ".join([quoteBranch(b) for b in args])), 
                  lambda:commands.push(ui, repo, branch=args, **pushOpts)):
      return 1

    # And return to the original topic branch
    if repo.dirstate.branch() != mergeFrom:
      if tryCommand(ui, "update %s" % quoteBranch(mergeFrom), lambda:hg.update(repo, mergeFrom)):
        return 1

    ui.status("Done.\n")

  # When all done, return to the original topic branch (even if something went wrong)
  finally:
    if repo.dirstate.branch() != mergeFrom and not commitStop:
      ui.pushbuffer()
      hg.update(repo, mergeFrom)
      ui.popbuffer()


###############################################################################
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

  for branch in branches:
    if not repo.branchheads(branch) or branch in repo.topicSpecialBranches:
      ui.warn("Error: %s is not an open topic branch\n" % branch)
      return 1

  if 'tmenu' in opts:
    if ui.prompt("Branch '%s': close it?" % branches[0]).upper() != 'Y':
      return 1
    opts = {}

  for branch in branches:

    if branch != repo.dirstate.branch():
      if tryCommand(ui, "update %s" % quoteBranch(branch), lambda:commands.update(ui, repo, node=branch)):
        return 1

    branchState = calcBranchState(repo, branch, repo.dirstate.parents()[0])
    if branchState.replace("*", "") not in ('local', repo.topicProdBranch):
      ui.warn("Error: branch has only been partly pushed. " +
              "Closing it would leave permanent diffferences between dev, stage and prod. " +
              "If you really need to get rid of the changes, they need to be reversed, " +
              "committed, and pushed all the way before closing.\n")
      return 1

    # Unlike a normal hg commit, if no text is specified we supply a reasonable default.
    branch = repo.dirstate.branch()
    text = opts.get('message')
    if text is None:
      text = "Closing %s" % branch

    # Close it
    if tryCommand(ui, "commit --close-branch", lambda:repo.commit(text, extra = {'close':'True'}) is None):
      return 1

    # And push.
    pushOpts = copy.deepcopy(opts)
    if 'message' in pushOpts:
      del pushOpts['message']
    pushOpts['force'] = True
    if tryCommand(ui, "push -f -b %s" % quoteBranch(branch), lambda:commands.push(ui, repo, branch=(branch,), **pushOpts)):
      return 1

  # Finally, update to dev to avoid confusingly re-opening the closed branch
  if tryCommand(ui, "update %s" % repo.topicProdBranch, lambda:commands.update(ui, repo, node=repo.topicProdBranch)):
    return 1

  ui.status("Done.\n")



###############################################################################
def calcChangectxAncestor(self, ctx2):
  """ Special context ancestor calculation is needed for our unusual but very
      regularized merge structure. """

  repo = self._repo

  # Only do this for topic repos, and only for merges to dev/stage/prod.
  if not isTopicRepo(repo) or self.branch() not in (repo.topicDevBranch, repo.topicStageBranch, repo.topicProdBranch):
    return origCalcChangectxAncestor(self, ctx2)

  # If cached, save some time.
  cacheKey = (self.node(), ctx2.node())
  if cacheKey in repo.topicAncestorCache:
    return repo.topicAncestorCache[cacheKey]

  #print "  changectx.ancestor(%d, %d):" % (self.rev(), ctx2.rev())

  # Handy stuff to have around
  changelog = repo.changelog

  # Helper function to produce ancestors. Their rev nums are
  # produced, highest first.
  #
  def ancestorsOf(ctx):

    # Deal with workingctx
    node = ctx.node()
    if node is None:
      node = c2.parents()[0].node()

    # Init
    visit = [-changelog.rev(node)]
    min = None
    while len(visit):
      rev = -heapq.heappop(visit)
      node = changelog.node(rev)

      # Avoid duplicates
      if min is not None and rev >= min:
        continue
      if min is None or rev < min:
        min = rev

      # Got one
      yield rev

      # Add the parent(s) to our list of revisions to visit
      parents = changelog.parents(node)
      p1 = changelog.rev(parents[0])
      heapq.heappush(visit, -p1)

      if not parents[1] == nullrev:
        p2 = changelog.rev(parents[1])
        heapq.heappush(visit, -p2)

  # Find the most recent common ancestor by doing a merge-sort
  # of the two lists until we find the highest matching rev.
  #
  ancestors = set()
  s1, s2 = ancestorsOf(self), ancestorsOf(ctx2)
  c1, c2 = s1.next(), s2.next()
  while True:
    if c1 < c2:
      c1, s1, c2, s2 = c2, s2, c1, s1
    if c1 in ancestors:
      break
    ancestors.add(c1)
    c1 = s1.next()

  orig = origCalcChangectxAncestor(self, ctx2)
  if orig:
    #print "    ctxancestor(%d,%d)=%d (was %d)" % (self.rev(), ctx2.rev(), c1, orig.rev())
    assert c1 >= orig.rev()
  else:
    #print "    ctxancestor(%d,%d)=%d (was None)" % (self.rev(), ctx2.rev(), c1)
    pass

  # Cache for future use
  ret = context.changectx(repo, c1)
  repo.topicAncestorCache[cacheKey] = ret
  return ret


###############################################################################
def debugtopic(ui, repo, *args, **opts):
  """ internal consistency checks for the topic extension """

  ui.status("Checking ancestor algorithm...\n")
  log = repo.changelog
  for rev in log:
    parents = log.parentrevs(rev)
    if parents[1] < 0:
      continue
    ctx1 = repo[parents[0]]
    ctx2 = repo[parents[1]]
    ctxa = calcChangectxAncestor(ctx1, ctx2)
    ctxo = origCalcChangectxAncestor(ctx1, ctx2)
    if ctxa.node() != ctxo.node():
      ui.status("  diff: ancestor(%d,%d)=%d (was %d)\n" % (ctx1.rev(), ctx2.rev(), ctxa.rev(), ctxo.rev()))

  ui.status("Done.")

###############################################################################
def tsync(ui, repo, *args, **opts):
  """ synchronize (pull & update) the current repo; also the topic extension itself. """

  mustBeTopicRepo(repo)

  # Pull and update the current repo
  if tryCommand(ui, "pull -u", lambda:commands.pull(ui, repo, **opts)):
    return 1

  # Then pull and update the topic extension
  topicDir = os.path.dirname(__file__)
  timeBefore = os.path.getmtime(os.path.join(topicDir, ".hg"))
  if tryCommand(ui, "pull -R %s -u" % quoteBranch(topicDir), lambda:os.system('hg pull -R "%s" --quiet -u' % topicDir)):
    return 1
  timeAfter = os.path.getmtime(os.path.join(topicDir, ".hg"))

  if timeBefore != timeAfter:
    ui.status("Note: Topic extension has been updated.\n")
    if 'tmenu' in opts:
      ui.status("...restarting menu.\n")
      os.system("hg tmenu")
      sys.exit(0)
      

###############################################################################
def tmenu(ui, repo, *args, **opts):
  """ menu-driven interface to the 'topic' extension """

  global waitingForInput

  mustBeTopicRepo(repo)

  menuEntries = [('o', topen,     "open new branch"),
                 ('l', tlog,      "log (history)"),
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
        ui.warn(e)
        ui.warn("\n")
    else:
      ui.status("Unknown option: '%s'\n" % resp)
      printFull = True


###############################################################################
def tversion(ui, repo, *args, **opts):
  """ print out the version of the topic extension """

  ui.status("Topic extension version: %s\n" % topicVersion)


#################################################################################
def reposetup(ui, repo):
  """ Set up branch names we need. These are typically 'dev', 'stage', and 'prod'
      but they can be overridden in a config file.
  """

  repo.topicAncestorCache = {}
  repo.topicDevBranch = ui.config('topic', 'dev-branch', default = 'dev')
  repo.topicStageBranch = ui.config('topic', 'stage-branch', default = 'stage')
  repo.topicProdBranch = ui.config('topic', 'prod-branch', default = 'prod')
  repo.topicIgnoreBranches = re.split("[ ,]+", ui.config('topic', 'ignore-branches', default = 'default'))
  repo.topicSpecialBranches = [repo.topicDevBranch, repo.topicStageBranch, repo.topicProdBranch] + repo.topicIgnoreBranches


#################################################################################
def uisetup(ui):
  """ Install extra special handlers for the topic extension. """

  global origCalcFileAncestor, origCalcChangectxAncestor

  orig = getattr(context.changectx, 'ancestor')
  assert orig is not None
  if os.path.exists("disable_new_merge"): # used for before vs after tests
    sys.stderr.write("(new merge disabled)\n")
    pass
  elif orig != calcChangectxAncestor:
    origCalcChangectxAncestor = orig
    setattr(context.changectx, 'ancestor', calcChangectxAncestor)


#################################################################################
# Table of commands we're adding.
#
cmdtable = {
    "topen|tnew":    (topen,
                      [('C', 'clean', None, 'discard uncommitted changes (no backup)')],
                      "newbranchname"),

    "tbranch|tb":    (tbranch,
                      [('C', 'clean', None, 'discard uncommitted changes (no backup)')],
                      "[branchname]"),

    "tbranches|tbs": (tbranches,
                      [('c', 'closed', None, "include closed branches")],
                      ""),

    "tlog|tl":       (tlog,
                      [('a', 'all', None, "all topic branches, not just current"),
                       ('c', 'closed', None, "include closed branches")] \
                       + commands.templateopts,
                      "[branches]"),

    "tpush|tp":      (tpush,
                      [('m', 'message',   None, "use <text> as commit message instead of default 'Merge to <branch>'"),
                       ('P', 'nopull',    None, "don't pull current data from master"),
                       ('C', 'nocommit',  None, "merge only, don't commit or push")] 
                       + commands.remoteopts,
                      "dev|stage|prod"),

    "tclose":        (tclose,
                      [('m', 'message',   None, "use <text> as commit message instead of default 'Closing <branch>'")]
                      + commands.remoteopts,
                      "[branches]"),

    "tmenu":         (tmenu,
                      [],
                      ""),

    "tsync":         (tsync,
                      [],
                      ""),

    "debugtopic":    (debugtopic,
                      [],
                      ""),

    "tversion":      (tversion,
                      [],
                      ""),

}

