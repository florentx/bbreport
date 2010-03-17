#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib
import urllib2
import fnmatch
import optparse
import xmlrpclib

__version__ = '0.1.0.dev1'

NUMBUILDS = 6
DEFAULT_TIMEOUT = 2
MSG_MAXLENGTH = 60

baseurl = 'http://www.python.org/dev/buildbot/'

# Common statuses for Builds and Builders
S_BUILDING = 'building'
S_SUCCESS = 'success'
S_UNSTABLE = 'unstable'
S_FAILURE = 'failure'
S_EXCEPTION = 'exception'   # Build only (mapped to S_FAILURE)
S_OFFLINE = 'offline'       # Builder only (mapped to S_BUILDING)

BUILDER_STATUSES = (S_BUILDING, S_SUCCESS, S_UNSTABLE, S_FAILURE, S_OFFLINE)

# Regular expressions
RE_BUILD = re.compile('Build #(\d+)</h1>\r?\n?'
                      '<h2>Results:</h2>\r?\n?'
                      '<span class="([^"]+)">')
RE_BUILD_REVISION = re.compile('<li>Revision: (\d+)</li>')
RE_FAILED = re.compile('(\d+) tests? failed:((?:\r?\n? +([^\r\n]+))+)')
RE_TIMEOUT = re.compile('command timed out: ([^,]+)')
RE_STOP = re.compile('(process killed by .+)')
RE_BBTEST = re.compile('make: \*\*\* \[buildbottest\] (.+)')

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

    Build.result should be one of (S_SUCCESS, S_FAILURE, S_UNSTABLE).
    """
    _data = None
    _message = None

    def __init__(self, name, buildnum, *args):
        self.num = buildnum
        self._url = '%s/builders/%s/builds/' % (baseurl, urllib.quote(name))
        self.failed_tests = []
        if args:
            # Use the XMLRPC response
            assert len(args) == 7
            revision, result = args[3:5]
            self.revision = int(revision or buildnum)
            self.result = result
            if result in (S_EXCEPTION, S_FAILURE):
                self._message = ' '.join(args[5])
            if not revision:
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
        # retrieve num, revision, result
        text = urlread(self.url)
        if not text:
            self.revision = self.num
            return S_BUILDING
        match = RE_BUILD.search(text)
        if match:
            self.num = int(match.group(1))
            result = match.group(2).strip()
        else:
            result = S_BUILDING
        match = RE_BUILD_REVISION.search(text)
        if match:
            self.revision = int(match.group(1))
        else:
            self.revision = self.num
        return result

    def _parse_stdio(self):
        stdio = urlread(self.url + '/steps/test/logs/stdio')
        stdio = stdio.replace(HTMLNOISE, '')
        match = RE_FAILED.search(stdio)
        if match:
            count_tests = int(match.group(1))
            failed_tests = match.group(2).strip()
            self._message = ''
            self.failed_tests = failed_tests.split()
            assert len(self.failed_tests) == count_tests
            return

        match = (RE_BBTEST.search(stdio) or
                 RE_TIMEOUT.search(stdio) or
                 RE_STOP.search(stdio))
        if match:
            self._message = match.group(1).strip()
        else:
            self._message = self.result + ': something crashed'

    def get_message(self):
        # Parse stdio on demand
        if self.result in (S_SUCCESS, S_BUILDING):
            msg = self.result
        else:
            if self._message is None or 'test' in self._message:
                self._parse_stdio()
            if self.failed_tests:
                msg = '%s failed: %s' % (len(self.failed_tests),
                                         ' '.join(self.failed_tests))
            else:
                msg = self._message
        return msg

    def asdict(self):
        """
        Convert the object in an easy-serializable dict.
        """
        return dict(num=self.num, data=self.data)


def print_builder(name, results, quiet):
    # (build.revision, build.result, build.get_message)

    count = {S_SUCCESS: 0, S_FAILURE: 0}
    short = []
    long = []

    for rev, result, get_msg in results:
        if result == S_BUILDING:
            short.append(cformat(' *** ', _colors[result]))
            continue

        shortrev = '%5d' % rev
        if len(short) > 1:
            shortrev = shortrev[-3:]
        short.append(cformat(shortrev, _colors[result]))

        if result == S_SUCCESS:
            count[S_SUCCESS] += 1
        else:
            count[S_FAILURE] += 1
            long.append((rev, get_msg, _colors[result]))

    if quiet > 1:
        # Print only the colored buildbot names
        if 0 == count[S_SUCCESS] == count[S_FAILURE]:
            return S_OFFLINE
        last_status = results[0][1]
        if last_status in (S_SUCCESS, S_BUILDING):
            return last_status
        return S_FAILURE

    if count[S_SUCCESS] == 0:
        if count[S_FAILURE] == 0:
            builder_status = S_OFFLINE
            short = [cformat(' *** ', _colors[S_OFFLINE])] * 2
        else:
            builder_status = S_FAILURE
    elif count[S_FAILURE] > 0:
        builder_status = S_UNSTABLE
    else:
        builder_status = S_SUCCESS

    builder_color = _colors[builder_status]
    print cformat('%-26s' % name, builder_color), ', '.join(short),

    if quiet and long:
        # Print last failure or error.
        rev, get_msg, color = long[0]
        msg = get_msg()
        if len(msg) > MSG_MAXLENGTH:
            msg = msg[:MSG_MAXLENGTH - 3] + '...'
        print '- ' + cformat(msg, color)
    else:
        # Move to next line
        print

    if not quiet:
        for rev, get_msg, color in long:
            print ' %5d:' % rev, cformat(get_msg(), color)

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

        print cformat(status.title() + ':', _colors[status])
        for host, branches in sorted(platforms.items()):
            print '\t' + cformat(host, _colors[status]), ', '.join(branches)


def print_final(counts):
    totals = []
    for status in BUILDER_STATUSES:
        if counts[status]:
            totals.append(cformat(counts[status], _colors[status]))
    print 'Totals:',
    print ' + '.join(totals)


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
                      metavar='num', help='the build number of a buildslave')
    parser.add_option('-f', '--failures', dest='failures',
                      action='append', default=[],
                      metavar='test_xyz', help='the name of a failed test')
    parser.add_option('-q', '--quiet', default=0, action='count',
                      help='one line per builder, or group by status with -qq')

    options, args = parser.parse_args()
    #print options, args
    return options, args


def main():
    options, args = parse_args()

    # create the xmlrpc proxy to retrieve the build data
    proxy = xmlrpclib.ServerProxy(baseurl + 'all/xmlrpc')

    # create the list of builders
    names = proxy.getAllBuilders()
    builders = sorted((Builder(name) for name in names),
                      key=lambda b: (b.branch, b.name))

    if options.branches:
        branches = options.branches.split(',')
    else:
        branches = args

    # filter the builders according to the options
    if options.name:
        #names = options.name.split(',')
        pattern = fnmatch.translate(options.name)
        selected_builders = [builder for builder in builders
                             if re.match(pattern, builder.name, re.I)]

    elif branches:
        selected_builders = [builder for builder in builders
                             if builder.branch in branches]
    else:
        selected_builders = builders

    print 'Selected builders:', len(selected_builders), '/', len(builders)

    if options.quiet > 1:
        numbuilds = 2
        groups = dict((s, []) for s in BUILDER_STATUSES)
        print "... retrieving last build results"
    else:
        numbuilds = NUMBUILDS

    counters = dict((s, 0) for s in BUILDER_STATUSES)

    # loop through the builders and their builds
    for builder in selected_builders:
        # If the builder is working, the list is partial or empty.
        lastbuilds = proxy.getLastBuilds(str(builder), numbuilds)

        # Complete the list with tuples like (builder_name, -1).
        builds = [(str(builder), -1 - i)
                  for i in range(numbuilds - len(lastbuilds))]
        builds += reversed(lastbuilds)

        results = []
        for lbuild in builds:
            build = Build(*lbuild)

            if options.failures:
                # Parse the stdio logs
                build.get_message()
                if not set(options.failures) <= set(build.failed_tests):
                    continue

            # These data are accumulated in a list of results which is
            # passed to a printer function.  The same list may be used
            # to generate other kind of reports (e.g. HTML, XML, ...).

            results.append((build.revision, build.result, build.get_message))
            builder.builds[build.num] = build

        builder_status = print_builder(str(builder), results,
                                       quiet=options.quiet)
        counters[builder_status] += 1

        if options.quiet > 1:
            groups[builder_status].append(str(builder))

    if options.quiet > 1:
        print_status(groups)

    print_final(counters)

    return builders


if __name__ == '__main__':
    # set the builders var -- useful with python -i
    builders = main()
