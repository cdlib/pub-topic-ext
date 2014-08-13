#!/usr/bin/env python

# Get ready for Python 3.x
from __future__ import unicode_literals, print_function, absolute_import, division

# System modules
import os, re, shutil, subprocess, sys
from os import path
from os.path import join as pjoin, exists as pexists

try:
  (prodBranch, remoteDest) = sys.argv[1:]
except:
  sys.stderr.write("Usage: %s prodBranchName remoteDest\n" % os.path.basename(sys.argv[0]))
  sys.exit(1)

# Determine the original changeset we're on
tmp = subprocess.check_output("hg identify", shell=True)
m = re.match("^([0-9a-f]{12})([+]?) \(([^\)]+)\)", tmp)
assert m is not None, "hg identify produced unexpected output %r" % tmp
(origChangeset, isModified, origBranch) = m.groups()
if isModified != '':
  print("Uncommitted changes, cannot repair. To revert changes first, use 'hg update --clean'.")
  sys.exit(1)

# Pull prod changesets from central
print("Pulling %s changes from central repo." % prodBranch)
subprocess.check_call("hg pull -r %s %s" % (prodBranch, remoteDest), shell=True)

# While there are multiple heads to merge...
nChanges = 0
nHeads = 0
while True:

  # Find all the heads on the 'prod' branch
  tmp = subprocess.check_output("hg heads --template='{node}\\t{branch}\\n'", shell=True)
  prodNodes = []
  for line in tmp.strip().split("\n"):
    m = re.match("^([0-9a-z]+)\t(.*)$", line)
    assert m is not None, "Can't match line %r" % line
    (node, branch) = m.groups()
    if branch == prodBranch:
      prodNodes.append(node)

  # If there are two heads, we can attempt a fix.
  nHeads = len(prodNodes)
  if nHeads == 0:
    print("There isn't a '%s' branch -- is this a topic repository?" % prodBranch)
    sys.exit(1)
  elif nHeads == 1:
    break

  print("Multiple heads found: %s." % ", ".join(prodNodes))

  # Try to auto-merge the first two heads.
  (mergeFrom, mergeTo) = prodNodes[0:2]
  print("Updating to target head %s." % mergeTo)
  subprocess.check_call("hg update -C -r %s" % mergeTo, shell=True)

  print("Merging source head %s." % mergeFrom)
  try:
    subprocess.check_call("hg merge -r %s --tool internal:merge" % mergeFrom, shell=True)
  except:
    print("Merge failed! Reverting to original changeset.")
    subprocess.check_call("hg update --clean -r %s" % origChangeset, shell=True)
    print("...and exiting without repair.")
    sys.exit(1)

  # Commit it
  print("Committing merge.")
  subprocess.check_call("hg commit -m 'Auto-merging multiple %s heads.'" % prodBranch, shell=True)
  nChanges += 1

# Pull prod changesets from central
if nChanges == 0:
  if nHeads == 1:
    print("There's only one '%s' head. Nothing to repair." % prodBranch)
    sys.exit(0)
  else:
    print("Unable to repair.")
    sys.exit(1)

print("Pushing fix(es) to the central repository.")
try:
  subprocess.check_call("hg push -r %s %s" % (prodBranch, remoteDest), shell=True)
finally:
  if origBranch != prodBranch:
    print("Returning to original branch '%s'." % origBranch)
    subprocess.check_call("hg update -C -r %s" % origChangeset, shell=True)

print("Repair successful.")

