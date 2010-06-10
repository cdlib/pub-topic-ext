#!/usr/bin/env python

'''topic

Extensions for eScholarship-style topic branches.
'''

import re
from xml import sax
from StringIO import StringIO
from mercurial import commands, extensions, hg, patch, util
from mercurial.node import nullid

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
def tbranch(orig, ui, *args, **kwargs):
  print "tbranch command"


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
    # cmd name        function call
    "print-parents": (printparents,
                     # see mercurial/fancyopts.py for all of the command
                     # flag options.
                     [('s', 'short', None, 'print short form'),
                      ('l', 'long', None, 'print long form')],
                     "[options] REV"),
    "tbranch":       (tbranch,
                      [('s', 'short', None, "short form")],
                      "")
}

