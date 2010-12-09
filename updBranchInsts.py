#!/usr/bin/env python -u

# This script checks the open branches available in a given Mercurial
# repository, and creates an XTF directory for each one that is a clone
# of the main directory. It then ensures that Tomcat will dispatch to
# each branch separately, based on a prefix to the host name.
#
# Safe for incremental operation -- it will add or delete trees as
# needed for the branches.
#
# The script should be in the tomcat/bin directory so it can find all
# the things it needs.
#

# Get ready for Python 3.x
from __future__ import print_function, absolute_import, division
# Not unicode_literals because optparse can't handle them

# System imports
import os, re, subprocess, sys
from optparse import OptionParser, make_option
from os import path
from os.path import join as pjoin

# Parse the command line
usage = "usage: \%prog [-v] tomcatDir tomcatURL mainWebappName"
parser = OptionParser()
parser.add_option("-v", "--verbose", action = 'store_true', dest = "verbose")
(options, args) = parser.parse_args()
if len(args) != 2:
  parser.error("Incorrect number of arguments")

tomcatDir = args[0]
tomcatURL = args[1]
mainAppName = args[2]

# Figure out the base host name
tomcatHost = re.sub(":\d+", "", tomcatURL) # get rid of port name

hostTemplate = pjoin(tomcatDir, "hostTemplate")
appTemplate = pjoin(hostTemplate, mainAppName)

homedir = os.getenv("HOME")

###############################################################################
def do(cmd):
  """ Print a command, then execute it. """

  if options.verbose:
    print("$ " + cmd)
  result = subprocess.check_output(cmd, shell=True).strip()
  if options.verbose:
    print(result)
  return result

###############################################################################
def createHost(branch, hostDir, appDir):
  """ Does the work of creating a new branch host. """

  if options.verbose:
    print("Creating new host for branch '%s'" % branch)

  # Copy the template
  do("rsync -a --link-dest=%s/ %s/ %s/" % (hostTemplate, hostTemplate, hostDir))

  # Then update to this branch
  do("hg -R %s update -C %s" % (appDir, branch))


###############################################################################
def updateBranch(branch, hostDir, appDir):
  """ Does the work of creating or updating a branch clone. """

  # Create and populate the directory if not done yet
  if path.exists(hostDir):
    isNew = False
  else:
    createHost(branch, hostDir, appDir)
    isNew = True

  # Now perform update tasks
  changeTxt = do("hg -R %s pull -v -b %s -u" % (appDir, branch))
  changedDirs = set()
  for line in re.split("\r?\n", changeTxt):
    m = re.match("^getting ([^/]+)/.*$", line)
    if m:
      changedDirs.add(m.group(1))
  if options.verbose:
    print("Changed dirs:", changedDirs)

  # Compile Java code changes if necessary
  if isNew or "WEB-INF" in changedDirs:
    do("ant -f %s" % pjoin(appDir, "WEB-INF", "build.xml"))

  # Expand the YUI zip file if necessary
  if isNew or "static" in changedDirs:
    yuiZip = pjoin(appDir, "static", "yui.zip")
    yuiDir = pjoin(appDir, "static", "yui")
    if not path.exists(yuiDir) or path.getmtime(yuiDir) < path.getmtime(yuiZip):
      do("cd %s; rm -r yui; unzip yui.zip; touch yui" % pjoin(appDir, "static"))


###############################################################################
def dropBranch(branch):
  """ Deletes a branch that's now closed. """

  host = "%s.%s" % (branch, tomcatHost)
  hostDir = pjoin(tomcatDir, host)

  # First tell Tomcat to remove the host
  do("curl -ns %s/host-manager/remove?name=%s" % (tomcatURL, host))

  # Then remove the directory
  do("rm -r %s" % hostDir)

###############################################################################
def dropOldBranches(openBranches):
  """ Scan the Tomcat directory for old branch hosts, and remove them. """

  suffix = ".%s" % tomcatHost

  for filename in os.listdir(tomcatDir):
    if not filename.endswith(suffix):
      continue
    branch = filename[0:-len(suffix)]
    if not branch in openBranches:
      dropBranch(branch)

  sys.exit(2)

###############################################################################
def main():

  # Pull updates to the branch template, then get a list of its branches
  do("hg -R %s pull -u" % appTemplate)
  branchTxt = do("hg -R %s branches" % appTemplate)
  branches = set()
  for line in branchTxt.split("\n"):
    branch = line.split()[0]
    # Skip non-topic branches
    if re.match("^((\w+-)?(dev|stage|prod)|default)", branch):
      continue
    branches.add(line.split()[0])

  # Make sure the .netrc file exists so curl can get credentials for the login
  netrc = pjoin(os.getenv("HOME"), ".netrc")
  if not path.exists(netrc):
    print("~/.netrc is missing -- we need it so curl can get password for tomcat host-manager app")
    sys.exit(1)

  # Find out which hosts are running in Tomcat
  hostTxt = do("curl -ns %s/host-manager/list" % tomcatURL)
  assert "OK - Listed hosts" in hostTxt, "Unexpected response from Tomcat"

  runningHosts = set()
  for line in re.split("\r?\n", hostTxt)[1:]:
    if "localhost" in line:
      continue
    suffix = ".%s:" % tomcatHost
    if not line.endswith(suffix):
      print("Cannot understand Tomcat host line '%s'" % line)
      continue
    host = line[0:-len(suffix)]
    runningHosts.add(host)

  if options.verbose:
    print("Running hosts:", runningHosts)

  # For each open branch, update our host instance
  for branch in branches:
    host = "%s.%s" % (branch, tomcatHost)
    hostDir = pjoin(tomcatDir, host)
    appDir = pjoin(hostDir, mainAppName)

    updateBranch(branch, hostDir, appDir)

    # If not running a host for this, do it now.
    if not branch in runningHosts:
      do("curl -ns %s/host-manager/add?name=%s" % (tomcatURL, host))

  # Get rid of hosts for branches that have been closed
  dropOldBranches(branches)

###############################################################################
main()
