#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib
import urllib2
import fnmatch
import optparse
import xmlrpclib

__version__ = '0.1dev'

NUMBUILDS = 6
DEFAULT_TIMEOUT = 2
MSG_MAXLENGTH = 60

baseurl = 'http://www.python.org/dev/buildbot/'

# Common statuses for Builds and Builders
S_BUILDING = 'building'
S_SUCCESS = 'success'
S_FAILURE = 'failure'
S_EXCEPTION = 'exception'   # Build only (mapped to S_FAILURE)
S_UNSTABLE = 'unstable'     # Builder only (intermittent failures)
S_OFFLINE = 'offline'       # Builder only (mapped to S_BUILDING)

BUILDER_STATUSES = (S_BUILDING, S_SUCCESS, S_UNSTABLE, S_FAILURE, S_OFFLINE)

# Regular expressions
RE_BUILD = re.compile('Build #(\d+)</h1>\r?\n?'
                      '<h2>Results:</h2>\r?\n?'
                      '<span class="([^"]+)">([^<]+)</span>')
RE_BUILD_REVISION = re.compile('<li>Revision: (\d+)</li>')
RE_FAILED = re.compile('(\d+) tests? failed:((?:\r?\n? +([^\r\n]+))+)')
RE_TIMEOUT = re.compile('command timed out: (\d+) ([^,]+)')
RE_STOP = re.compile('(process killed by .+)')
RE_BBTEST = re.compile('make: \*\*\* \[buildbottest\] (.+)')

# Buildbot errors
OSERRORS = ('filesystem is full', 'Cannot allocate memory')

# HTML pollution in the stdio log
HTMLNOISE = '</span><span class="stdout">'

# Colored output
_shell_colors = {'black':   '30;01',
                 'red':     '31;01',
                 'green':   '32;01',
                 'yellow':  '33;01',
                 'blue':    '34;01'}

_colors = {S_SUCCESS: 'green', S_FAILURE: 'red', S_EXCEPTION: 'yellow',
           S_UNSTABLE: 'yellow', S_BUILDING: 'blue', S_OFFLINE: 'black'}

for status, color in _colors.items():
    _shell_colors[status] = _shell_colors[color]
del _colors


def cformat(text, color):
    return '\x1b[%sm%s\x1b[39;49;00m' % (_shell_colors[color], text)


def urlread(url):
    try:
        resource = urllib2.urlopen(url, timeout=DEFAULT_TIMEOUT)
        return resource.read()
    except IOError:
        return ''


class Builder(object):
    """
    Represent a builder.
    """
    def __init__(self, name):
        self.name = name
        # the branch name should always be the last part of the name
        self.host, self.branch = name.rsplit(None, 1)
        self.url = baseurl + 'builders/' + urllib.quote(name)
        self.builds = {}
        self.lastbuild = 0

    @classmethod
    def fromdump(self, data):
        """
        Alternate contructor to create the object from a serialized builder.
        """
        pass

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return self.name

    def asdict(self):
        """
        Convert the object in an easy-serializable dict.
        """
        builds = dict((num, build.asdict())
                      for num, build in self.builds.iteritems())
        return dict(name=self.name, builds=builds)


class Build(object):
    """
    Represent a single build of a builder.

    Build.result should be one of (S_SUCCESS, S_FAILURE, S_EXCEPTION).
    If the result is not available, it defaults to S_BUILDING.
    """
    _data = None
    _message = None

    def __init__(self, name, buildnum, *args):
        self.num = buildnum
        self.revision = 0
        self._url = '%s/builders/%s/builds/' % (baseurl, urllib.quote(name))
        self.failed_tests = []
        if args:
            # Use the XMLRPC response
            assert len(args) == 7
            revision, result = args[3:5]
            if result in (S_EXCEPTION, S_FAILURE):
                # Store the failure details
                self._message = ' '.join(args[5])
            if revision:
                self.revision = int(revision)
                self.result = result
            else:
                # Some buildbots hide the revision
                self.result = self._parse_build()
        else:
            # Fallback to the web page
            self.result = self._parse_build()

    @classmethod
    def fromdump(self, data):
        """
        Alternate contructor to create the object from a serialized builder.
        """
        pass

    @property
    def url(self):
        return self._url + str(self.num)

    @property
    def data(self):
        return self._data

    def _parse_build(self):
        # retrieve num, result, revision and message
        build_page = urlread(self.url)
        if not build_page:
            return S_BUILDING
        match = RE_BUILD.search(build_page)
        if match:
            self.num = int(match.group(1))
            result = match.group(2)
            self._message = match.group(3)
        else:
            result = S_BUILDING
        match = RE_BUILD_REVISION.search(build_page)
        if match:
            self.revision = int(match.group(1))
        return result

    def _parse_stdio(self):
        stdio = urlread(self.url + '/steps/test/logs/stdio')
        stdio = stdio.replace(HTMLNOISE, '')

        # Check if some test failed
        fail = RE_FAILED.search(stdio)
        if fail:
            failed_count = int(fail.group(1))
            failed_tests = fail.group(2).strip()
            self.failed_tests = failed_tests.split()
            assert len(self.failed_tests) == failed_count

        lines = stdio.splitlines()

        # Check if disk full or out of memory
        for line in lines:
            for error in OSERRORS:
                if error in line:
                    break
            else:
                continue
            self.result = S_EXCEPTION
            self._message = error.lower()
            break
        else:
            self._message = error = ''

        if fail or error:
            # If something is found, stop here
            return

        self._message = 'something crashed'
        reversed_lines = reversed(lines)
        for line in reversed_lines:
            killed = RE_BBTEST.search(line) or RE_STOP.search(line)
            if killed:
                self._message = killed.group(1).strip().lower()
                # Check previous line for a possible timeout
                line = next(reversed_lines)

            timeout = RE_TIMEOUT.search(line)
            if timeout:
                minutes = int(timeout.group(1)) // 60
                # It is a test failure
                self.result = S_FAILURE
                self._message = 'hung for %d min' % minutes
                # Move to previous line
                line = next(reversed_lines)

            if line.startswith('test_'):
                # This is the last running test
                self.failed_tests = [line]
                break

    def get_message(self):
        # Parse stdio on demand
        if self.result in (S_SUCCESS, S_BUILDING):
            msg = self.result
        else:
            if self._message is None or 'test' in self._message:
                self._parse_stdio()
            msg = self._message
            if self.failed_tests:
                if self.result == S_EXCEPTION:
                    # disk full or other buildbot error
                    msg += ' (%s failed)' % len(self.failed_tests)
                elif msg:
                    # process killed: print last test
                    msg += ': ' + ' '.join(self.failed_tests)
                else:
                    # test failures
                    msg = '%s failed: %s' % (len(self.failed_tests),
                                         ' '.join(self.failed_tests))
        return msg

    def asdict(self):
        """
        Convert the object in an easy-serializable dict.
        """
        return dict(num=self.num, data=self.data)


def print_builder(name, builds, quiet):

    count = {S_SUCCESS: 0, S_FAILURE: 0}
    capsule = []
    failed_builds = []

    for build in builds:
        result = build.result
        compact = (quiet or len(builds) > 6) and len(capsule) > 1

        if build.revision:
            revision = '%5d' % build.revision
            rev = revision if not compact else revision[-3:]
        else:
            rev = ' *** ' if not compact else '***'
        capsule.append(cformat(rev, result))

        if result == S_BUILDING:
            continue
        elif result == S_SUCCESS:
            count[S_SUCCESS] += 1
        else:
            count[S_FAILURE] += 1
            failed_builds.append(build)

    if quiet > 1:
        # Print only the colored buildbot names
        if 0 == count[S_SUCCESS] == count[S_FAILURE]:
            return S_OFFLINE
        last_result = builds[0].result
        if last_result in (S_SUCCESS, S_BUILDING):
            return last_result
        return S_FAILURE

    if count[S_SUCCESS] == 0:
        if count[S_FAILURE] == 0:
            builder_status = S_OFFLINE
            capsule = [cformat(' *** ', S_OFFLINE)] * 2
        else:
            builder_status = S_FAILURE
    elif count[S_FAILURE] > 0:
        builder_status = S_UNSTABLE
    else:
        builder_status = S_SUCCESS

    print cformat('%-26s' % name, builder_status), ', '.join(capsule),

    if quiet and failed_builds:
        # Print last failure or error.
        build = failed_builds[0]
        msg = build.get_message()
        if len(msg) > MSG_MAXLENGTH:
            msg = msg[:MSG_MAXLENGTH - 3] + '...'
        print '- ' + cformat(msg, build.result)
    else:
        # Move to next line
        print

    if not quiet:
        for build in failed_builds:
            msg = build.get_message()
            print ' %5d:' % build.revision, cformat(msg, build.result)

    return builder_status


def print_status(groups):
    for status in BUILDER_STATUSES:
        names = groups[status]
        if not names:
            continue
        platforms = {}
        for name in names:
            host, branch = name.rsplit(None, 1)
            platforms.setdefault(host, []).append(branch)

        print cformat(status.title() + ':', status)
        for host, branches in sorted(platforms.items()):
            print '\t' + cformat(host, status), ', '.join(branches)


def print_final(counts):
    totals = []
    for status in BUILDER_STATUSES:
        if counts[status]:
            totals.append(cformat(counts[status], status))
    print 'Totals:', ' + '.join(totals)


def parse_args():
    """
    Create an option parser, parse the result and return options and args.
    """
    parser = optparse.OptionParser(version=__version__,
                                   usage="%prog [options] branch ...")
    parser.add_option('-n', '--name', dest='name', default=None,
                      metavar='NAME', help='buildbot name')
    parser.add_option('-b', '--branches', dest='branches', default=None,
                      metavar='BRANCHES',
                      help='the Python branches (e.g. 2.6,3.1)')
    parser.add_option('-u', '--build', dest='build', default=None,
                      metavar='num', help='the build number of a buildslave'
                                          ' (not implemented)')
    parser.add_option('-f', '--failures', dest='failures',
                      action='append', default=[],
                      metavar='test_xyz', help='the name of a failed test')
    parser.add_option('-q', '--quiet', default=0, action='count',
                      help='one line per builder, or group by status with -qq')

    options, args = parser.parse_args()

    if options.failures:
        # Ignore the -q option
        options.quiet = 0

    #print options, args
    return options, args


def main():
    options, args = parse_args()

    # create the xmlrpc proxy to retrieve the build data
    proxy = xmlrpclib.ServerProxy(baseurl + 'all/xmlrpc')

    # create the list of builders
    names = proxy.getAllBuilders()

    # sort by branch and name
    builders = sorted((Builder(name) for name in names),
                      key=lambda b: (b.branch, str(b)))

    if options.branches:
        branches = options.branches.split(',')
    else:
        branches = args

    if branches:
        # filter by branch
        selected_builders = [builder for builder in builders
                             if builder.branch in branches]
    else:
        selected_builders = builders

    if options.name:
        # filter by name
        pattern = fnmatch.translate(options.name)
        selected_builders = [builder for builder in selected_builders
                             if re.match(pattern, builder.name, re.I)]

    print 'Selected builders:', len(selected_builders), '/', len(builders)

    if options.quiet > 1:
        # For the "-qq" option, 2 builds per builder is enough
        numbuilds = 2
        groups = dict((s, []) for s in BUILDER_STATUSES)
        print "... retrieving last build results"
    elif not options.quiet and len(selected_builders) < 3:
        # show more builds
        numbuilds = NUMBUILDS * 2
    else:
        numbuilds = NUMBUILDS

    if options.failures:
        print "... retrieving build results"

    counters = dict((s, 0) for s in BUILDER_STATUSES)

    # loop through the builders and their builds
    for builder in selected_builders:
        # If the builder is working, the list may be partial or empty.
        xmlrpcbuilds = proxy.getLastBuilds(str(builder), numbuilds)

        # Fill the list with tuples like (builder_name, -1).
        lastbuilds = [(str(builder), -1 - i)
                      for i in range(numbuilds - len(xmlrpcbuilds))]
        lastbuilds += reversed(xmlrpcbuilds)

        # default value is True without "-f" option
        found_failure = not options.failures

        builds = []
        for build_info in lastbuilds:
            build = Build(*build_info)

            if not found_failure:
                # Retrieve the failed tests
                build.get_message()
                if set(options.failures) <= set(build.failed_tests):
                    found_failure = True

            # These data are accumulated in a list of results which is
            # passed to a printer function.  The same list may be used
            # to generate other kind of reports (e.g. HTML, XML, ...).

            builds.append(build)
            builder.builds[build.num] = build

        if not found_failure:
            # no build matched the options.failures
            continue

        builder_status = print_builder(str(builder), builds,
                                       quiet=options.quiet)

        if options.quiet > 1:
            groups[builder_status].append(str(builder))

        counters[builder_status] += 1

    if options.quiet > 1:
        print_status(groups)

    print_final(counters)

    return builders


if __name__ == '__main__':
    # set the builders var -- useful with python -i
    builders = main()
