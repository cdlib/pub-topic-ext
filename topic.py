#!/usr/bin/env python

'''topic

Extensions for eScholarship-style topic branches.
'''

import os, re, sys
from xml import sax
from StringIO import StringIO
from mercurial import cmdutil, commands, extensions, hg, patch, url, util
from mercurial.node import nullid, nullrev


#################################################################################
def ruleError(ui, message):
  """ Convenience function: Output message through ui.warn(), and return False. """

  ui.warn("Error: %s\n" % message)
  return False


#################################################################################
def isTopicRepo(repo):
  """ Check if the given repo is likely a topic repo. """

  bm = repo.branchmap()
  return 'dev' in bm and 'stage' in bm and 'prod' in bm


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
      branchMap = repo.branchmap()
      assert reqPred in branchMap
      reqHead = branchMap[reqPred][0]
      ancestor = repo[reqHead].ancestor(ctx)

      # If there have been changes to the feature since the last merge to the predecessor,
      # that means somebody forgot to do the missing merge.
      #
      if ancestor != parent2:
        return ruleError(ui, "You must merge changes to %s before %s." % (reqPred, thisBranch))

  else:

    # Non-merge. These plain commits are not allowed on dev/stage/prod.
    if thisBranch in ('dev', 'stage', 'prod'):
      return ruleError(ui, "Direct commits to dev/stage/prod not allowed. Try making a topic branch.")

  return True # no problems.


#################################################################################
def validateChangegroup(ui, repo, hooktype, **opts):

  """ pretxnchangegroup hook: perform content-specific checks before accepting 
      an incoming changegroup. """

  # Silently skip this for non-topic repos
  if not isTopicRepo(repo):
    return 0

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
  """ Yield all revisions of the given branch, in topological order. """

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
def findMainMerge(repo, topicRevs, mainBranch):
  """ Figure out whether a local branch has been merged to a main branch. """

  # Handle brand new branch
  if not topicRevs:
    return None

  # Determine where to stop the scan
  firstTopicRev = int(topicRevs[-1])

  # Scan for merges from the topic branch to the given main branch
  topicSet = set(topicRevs)
  for mr in revsOnBranch(repo, mainBranch):

    # Stop if we reach a point in history before the start of the topic
    if int(mr) < int(topicRevs[-1]):
      break

    # Skip non-merges
    if len(mr.parents()) < 2:
      continue

    # Is this a marge from our topic branch? If so we're done.
    if mr.parents()[1] in topicSet:
      return mr.parents()[1]
      break

  # Couldn't find anything.
  return None


#################################################################################
def calcBranchState(repo, branch, ctx):
  """ Figure out whether the given branch is local, or has been merged to
      dev/stage/prod. """

  # Detect if the branch has been closed
  if 'close' in ctx.changeset()[5]:
    states = {ctx: 'closed'}
  else:
    states = {ctx: 'local'}

  # Now check for merges to dev, stage, and prod
  topicRevs = list(revsOnBranch(repo, branch))
  for mainBr in ('dev', 'stage', 'prod'):
    found = findMainMerge(repo, topicRevs, mainBr)
    if found:
      states[found] = mainBr

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
  if target in topicBranchNames(repo, closed=True) + ['dev', 'prod', 'stage']:
    ui.warn("Error: a branch with that name already exists; try choosing a different name.\n")
    return 1

  if repo.dirstate.branch() != 'dev':
    ui.status("Branching from dev...\n")
    res = commands.update(ui, repo, node='dev', check=True)
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
    ui.status("        Status: %s\n" % calcBranchState(repo, repo.dirstate.branch(), repo[repo.dirstate.parents()[0]]))
    return

  # Otherwise, switch to a different (existing) branch
  target = args[0]
  if target == repo.dirstate.branch():
    ui.status("You're already on that branch.\n")
    return

  if not target in topicBranchNames(repo, closed=True) + ['dev', 'prod', 'stage']:
    ui.warn("Error: branch '%s' does not exist.\n" % target)
    return 1

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

  ret = [tup[0] for tup in topicBranches(repo, closed)]

  rb = repo.dirstate.branch()
  if rb not in ret and rb not in ('dev', 'stage', 'prod', 'default'):
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
    branchState = calcBranchState(repo, branch, ctx)

    descrip = ctx.description().splitlines()[0].strip()
    if len(descrip) > 60:
      descrip = descrip[0:57] + "..."

    toPrint.append((branch, user, date, branchState, descrip))

  if repo.dirstate.branch() not in branches and \
     repo.dirstate.branch() not in ('dev', 'stage', 'prod', '', 'default'):
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
def tryCommand(ui, descrip, commandFunc):
  """ Run a command and capture its output. Print the output only if it fails. """

  ui.status(descrip)
  ui.flush()

  # For some reason push isn't responsive to ui.pushbuffer, so we have to work
  # harder to grab its output.
  #
  buffer = StringIO()
  (oldStdout, oldStderr) = (sys.stdout, sys.stderr)
  (sys.stdout, sys.stderr) = (buffer, buffer)

  # Now run the command
  res = 1
  try:

    res = commandFunc()

  finally:

    # Restore in/out streams
    (sys.stdout, sys.stderr) = (oldStdout, oldStderr)
    outFunc = ui.warn if res else ui.note
    outFunc("\n")
    for line in buffer.getvalue().split("\n"):
      outFunc("    " + line + "\n")

  # All done; let the caller know how it went.
  return res


###############################################################################
def onTopicBranch(ui, repo):
  """ Check if the repo is currently on a topic branch. """

  branch = repo.dirstate.branch()
  if branch in ('dev', 'stage', 'prod', 'default'):
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
def tpush(ui, repo, *args, **opts):
  """ merge current branch to dev, stage, or production and push it there """

  mustBeTopicRepo(repo)

  # Sanity checks
  if not(onTopicBranch(ui, repo) and isClean(ui, repo)):
    return 1
  mergeFrom = repo.dirstate.branch()

  # If called from the menu, allow user to choose target branch
  if 'tmenu' in opts:
    resp = ui.prompt("Which one [DSP]:", "")
    if resp.upper() not in ('D', 'S', 'P'):
      ui.warn("Unknown push target '%s'" % resp)
      return 1
    resp = resp.upper()
    args = ['dev' if resp=='D' else 'stage' if resp == 'S' else 'prod']
    opts = { 'nopull':False, 'nocommit':False, 'message':None }

  # Make sure a branch to push to was specified
  elif len(args) < 1:
    ui.warn("Error: You must specify at least one branch (dev/stage/prod) to push to.\n")
    return 1

  # Figure out where it's currently pushed to
  pushedTo = [p.branch() for p in repo.parents()[0].children()]

  # Make sure the arguments are in order (dev -> stage -> prod)
  tmp = set(pushedTo)
  alreadyMerged = set()
  for arg in args:
    need = {'dev':None, 'stage':'dev', 'prod':'stage'}.get(arg, 'bad')
    if need == 'bad':
      ui.warn("Error: you may only push to 'dev', 'stage', or 'prod'.\n")
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

    for mergeTo in args:

      # Pull new changes from the central repo
      if not opts['nopull'] and not pulled and set(args) - alreadyMerged:
        if tryCommand(ui, "pull ... ", lambda:commands.pull(ui, repo, **opts)):
          return 1
        pulled = True

      # Merge if necessary
      if not mergeTo in alreadyMerged:

        # Update to the branch we're going to merge into
        if tryCommand(ui, "update %s ... " % mergeTo, lambda:hg.update(repo, mergeTo)):
          return 1

        # Merge it.
        if tryCommand(ui, "merge %s ... " % mergeFrom, lambda:hg.merge(repo, mergeFrom)):
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
        if tryCommand(ui, "commit ... ", lambda:repo.commit(text) is None):
          return 1

      # Push to the central server
      pushOpts = opts
      pushOpts['force'] = True # we very often create new branches and it's okay!
      if tryCommand(ui, "push -fb %s ... " % mergeTo, 
                    lambda:commands.push(ui, repo, branch=(mergeTo,), **pushOpts)):
        return 1

    # And return to the original topic branch
    if repo.dirstate.branch() != mergeFrom:
      if tryCommand(ui, "update %s ... " % mergeFrom, lambda:hg.update(repo, mergeFrom)):
        return 1

    ui.status("done.\n")

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
    if not repo.branchheads(branch) or branch in ('default', 'dev', 'stage', 'prod'):
      ui.warn("Error: %s is not an open topic branch\n" % branch)
      return 1

  if 'tmenu' in opts:
    if ui.prompt("Branch '%s': close it?" % branches[0]).upper() != 'Y':
      return 1
    opts = {}

  for branch in branches:

    if branch != repo.dirstate.branch():
      if tryCommand(ui, "update %s ... " % branch, lambda:commands.update(ui, repo, node=branch)):
        return 1

    # Unlike a normal hg commit, if no text is specified we supply a reasonable default.
    branch = repo.dirstate.branch()
    text = opts.get('message')
    if text is None:
      text = "Closing %s" % branch

    # Close it
    if tryCommand(ui, "commit --close-branch ... ", lambda:repo.commit(text, extra = {'close':'True'}) is None):
      return 1

    # And push.
    if tryCommand(ui, "push -b %s ... " % branch, lambda:commands.push(ui, repo, branch=(branch,), **opts)):
      return 1

  # Finally, update to dev to avoid confusingly re-opening the closed branch
  if tryCommand(ui, "update dev ... ", lambda:commands.update(ui, repo, node='dev', **opts)):
    return 1

  ui.status("done.\n")


###############################################################################
def tmenu(ui, repo, *args, **opts):
  """ menu-driven interface to the 'topic' extension """

  menuEntries = [('O', topen,     "Open new branch"),
                 ('L', tlog,      "Log (history)"),
                 ('P', tpush,     "Push to dev/stage/prod"),
                 ('C', tclose,    "Close branch"),
                 ('S', tbranches, "Show open branches"),
                 ('B', tbranch,   "Branch switch"),
                 ('Q', None,      "Quit menu")]
  cols = ([], [])
  colSizes = [0, 0]
  colNum = 0
  div = len(menuEntries) / 2
  for i in range(len(menuEntries)):
    ent = menuEntries[i]
    str = "[%c] %s" % (ent[0], ent[2])
    cols[colNum].append(str)
    colSizes[colNum] = max(colSizes[colNum], len(str))
    if i == div:
      colNum += 1
  cols[colNum].append("") # in case of odd number

  # Loop until user quits
  printFull = True
  while True:

    # Print the whole menu the first time, or after an unknown choice
    if printFull:
      ui.status("\n")
      tbranch(ui, repo)
      ui.status("\nTopic branch menu:\n\n")
      for i in range(len(cols[0])):
        ui.status("    %-*s    %-*s\n" % (colSizes[0], cols[0][i], colSizes[1], cols[1][i]))
      printFull = False

    allChoices = "".join([ent[0] for ent in menuEntries])
    resp = ui.prompt("\nCommand [%s or ?]:" % allChoices, default='')

    # Handle quit separately
    if resp.upper() == 'Q':
      quit = True
      break

    # Handle '?'
    if resp == '?' or resp == '':
      printFull = True
      continue

    # Execute the corresponding command
    found = [ent for ent in menuEntries if resp.upper() == ent[0]]
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

}

