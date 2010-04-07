#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib
import urllib2
import fnmatch
import optparse
import os
import shutil
import sqlite3
import sys
import xmlrpclib

__version__ = '0.1dev'

NUMBUILDS = 6
DEFAULT_BRANCHES = 'all'
DEFAULT_TIMEOUT = 2
MSG_MAXLENGTH = 60
DEFAULT_OUTPUT = {
    # keywords: <ansi color>, bright, bold
    # use an empty string to preserve terminal settings
    'foreground': 'white bright',
    'background': 'black',
    # set to False to disable colors
    'color': True,
}
ANSI_COLOR = ('black', 'red', 'green', 'yellow',
              'blue', 'magenta', 'cyan', 'white')

baseurl = 'http://www.python.org/dev/buildbot/'
dbfile = os.path.splitext(__file__)[0] + '.sqlite'

# Global connection
conn = None

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
RE_TEST = re.compile('(test_[^ ]+)$')

# Buildbot errors
OSERRORS = ('filesystem is full',
            'No space left on device',
            'Cannot allocate memory')

# HTML pollution in the stdio log
HTMLNOISE = '</span><span class="stdout">'

# Format output
SYMBOL = {'black': '.', 'red': '#', 'green': '_', 'yellow': '?', 'blue': '*'}

_colors = {S_SUCCESS: 'green', S_FAILURE: 'red', S_EXCEPTION: 'yellow',
           S_UNSTABLE: 'yellow', S_BUILDING: 'blue', S_OFFLINE: 'black'}


def _prepare_output():
    default_fg = DEFAULT_OUTPUT['foreground'].lower()
    default_bg = DEFAULT_OUTPUT['background'].lower()
    _base = '\x1b[1;' if ('bold' in default_fg) else '\x1b['
    fg_offset = 90 if ('bright' in default_fg) else 30
    bg_offset = 100 if ('bright' in default_bg) else 40
    fg_color = next((fg_offset + idx for (idx, color) in enumerate(ANSI_COLOR)
                     if color in default_fg), 39)
    bg_color = next((bg_offset + idx for (idx, color) in enumerate(ANSI_COLOR)
                     if color in default_bg), 49)

    for status, color in _colors.items():
        SYMBOL[status] = SYMBOL[color]
        _escape_sequence[status] = '%s%s;%sm%%s\x1b[%sm' % \
            (_base, fg_offset + ANSI_COLOR.index(color), bg_color, fg_color)


_escape_sequence = {}
_prepare_output()
del _colors, _prepare_output


def _cformat_plain(text, color, sep=' '):
    return sep.join((SYMBOL[color], str(text)))


def _cformat_color(text, color, sep=None):
    return _escape_sequence[color] % text


def reset_terminal():
    if cformat == _cformat_color:
        # reset terminal colors
        print '\x1b[39;49;00m',
    print

cformat = _cformat_color if DEFAULT_OUTPUT['color'] else _cformat_plain


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
    saved = status = None
    lastbuild = 0

    def __init__(self, name):
        self.name = name
        # the branch name should always be the last part of the name
        self.host, self.branch = name.rsplit(None, 1)
        self.url = baseurl + 'builders/' + urllib.quote(name)
        self.builds = {}
        self._load_builder()
        if not self.saved:
            self.save()

    @classmethod
    def fromdump(cls, data):
        """
        Alternate contructor to create the object from a serialized builder.
        """
        pass

    @classmethod
    def query_all(cls):
        if conn is None:
            return []
        cur = conn.execute('select builder from builders')
        return [cls(name) for (name,) in cur.fetchall()]

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return self.name

    def _load_builder(self):
        if conn is None:
            return
        row = conn.execute('select lastbuild, status from builders where '
                           'builder = ? ', (self.name,)).fetchone()
        if row is not None:
            self.saved = True
            (self.lastbuild, self.status) = row

    def add(self, *builds):
        last = self.lastbuild
        for build in builds:
            self.builds[build.num] = build
            last = max(last, build.num)
        if last > self.lastbuild:
            self.lastbuild = last
            self.save()

    def save(self):
        if conn is None:
            return
        if self.saved:
            conn.execute('update builders set lastbuild = ?, status = ? '
                         'where builder = ?',
                         (self.lastbuild, self.status, self.name))
        else:
            conn.execute('insert into builders(builder, host, branch, '
                         'lastbuild, status) values (?, ?, ?, ?, ?)',
                         (self.name, self.host, self.branch,
                          self.lastbuild, self.status))
            self.saved = True
        return True

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
    _data = _message = saved = result = None
    revision = 0

    def __init__(self, name, buildnum, *args):
        self.builder = name
        self.num = buildnum
        self._url = '%s/builders/%s/builds/' % (baseurl, urllib.quote(name))
        self.failed_tests = []
        if buildnum:
            # Query the database
            self.result = self._load_build()
        if self.result:
            return
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
        if not self.result:
            # Fallback to the web page
            self.result = self._parse_build()
        if self._message in ('failed svn',):
            self.result = S_EXCEPTION
        elif self.result == S_SUCCESS:
            self.save()

    @classmethod
    def fromdump(cls, data):
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

    def save(self):
        if conn is None or self.saved:
            return
        if self.result not in (S_SUCCESS, S_FAILURE, S_EXCEPTION):
            return False
        conn.execute('insert into builds(builder, build, revision, result, '
                     'message) values (?, ?, ?, ?, ?)', (self.builder,
                     self.num, self.revision, self.result, self._message))
        if self.failed_tests:
            lines = ((self.builder, self.num, test)
                     for test in self.failed_tests)
            conn.executemany('insert into failures(builder, build, failed) '
                             'values (?, ?, ?)', lines)
        self.saved = True
        return True

    def _load_build(self):
        # retrieve revision, result and message
        result = None
        if conn is not None and self.num > 0:
            row = conn.execute('select revision, result, message from builds'
                               ' where builder = ? and build = ?',
                               (self.builder, self.num)).fetchone()
            if row is not None:
                self.saved = True
                (self.revision, result, self._message) = row
        return result

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
        self._load_build()
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
            error = next((e for e in OSERRORS if e in line), None)
            if error is None:
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

            failed = RE_TEST.match(line)
            if failed:
                # This is the last running test
                self.failed_tests = [failed.group(1)]
                break
        else:
            # No test failure: probably a buildbot error
            self.result = S_EXCEPTION

    def get_message(self):
        if self.result in (S_SUCCESS, S_BUILDING):
            return self.result
        if self.saved and conn is not None:
            cur = conn.execute('select failed from failures where '
                               'builder = ? and build = ?',
                               (self.builder, self.num))
            self.failed_tests = [test for (test,) in cur.fetchall()]
        else:
            if self._message is None or 'test' in self._message:
                # Parse stdio on demand
                self._parse_stdio()
            self.save()
        msg = self._message
        if self.failed_tests:
            count_failed = len(self.failed_tests)
            if self.result == S_EXCEPTION and count_failed > 2:
                # disk full or other buildbot error
                msg += ' (%s failed)' % count_failed
            elif msg:
                # process killed: print last test
                msg += ': ' + ' '.join(self.failed_tests)
            else:
                # test failures
                msg = '%s failed: %s' % (count_failed,
                                         ' '.join(self.failed_tests))
        return msg

    def asdict(self):
        """
        Convert the object in an easy-serializable dict.
        """
        return dict(num=self.num, data=self.data)


def load_database():
    global conn
    if conn is None:
        conn = sqlite3.connect(':memory:')
    if os.path.exists(dbfile):
        with open(dbfile, 'rb') as f:
            conn.executescript(f.read())
    else:
        conn.execute('create table builders'
                     '(builder, host, branch, lastbuild, status)')
        conn.execute('create table builds'
                     '(builder, build, revision, result, message)')
        conn.execute('create table failures'
                     '(builder, build, failed)')


def dump_database():
    if conn is None:
        return
    # Backup previous dump (and overwrite existing backup)
    if os.path.exists(dbfile):
        shutil.move(dbfile, dbfile + '.bak')
    # Dump the database
    with open(dbfile, 'wb') as f:
        f.writelines(l + os.linesep for l in conn.iterdump())
    # Close the connection
    conn.close()


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
        capsule.append(cformat(rev, result, sep=''))

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
            capsule = [cformat(' *** ', S_OFFLINE, sep='')] * 2
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
        print SYMBOL[build.result], msg
    else:
        # Move to next line
        print

    if not quiet:
        for build in failed_builds:
            msg = build.get_message()
            print ' %5d:' % build.revision, SYMBOL[build.result], msg

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
            totals.append(cformat(counts[status], status, sep=':'))
    print 'Totals:', ' + '.join(totals),


def parse_args():
    """
    Create an option parser, parse the result and return options and args.
    """
    global cformat

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
    parser.add_option('-o', '--offline', default=False, action='store_true',
                      help='use the local database')
    parser.add_option('--no-color', default=False, action='store_true',
                      help='do not color the output')
    parser.add_option('--no-database', default=False, action='store_true',
                      help='do not cache the result in a database file')

    options, args = parser.parse_args()

    if options.offline and options.no_database:
        print "--offline and --no-database don't go together"
        sys.exit(1)

    if options.failures:
        # ignore the -q option
        options.quiet = 0

    if options.no_color:
        # replace the colorizer
        cformat = _cformat_plain

    #print options, args
    return options, args


def main():
    global conn

    options, args = parse_args()

    if not options.no_database:
        # Load the database
        load_database()

    if options.offline:
        builders = Builder.query_all()
    else:
        # create the xmlrpc proxy to retrieve the build data
        proxy = xmlrpclib.ServerProxy(baseurl + 'all/xmlrpc')

        # create the list of builders
        builders = (Builder(name) for name in proxy.getAllBuilders())

    # sort by branch and name
    builders = sorted(builders, key=lambda b: (b.branch, str(b)))

    if options.branches:
        branches = options.branches.split(',')
    elif args:
        branches = args
    elif options.name:
        # there's a name filter defined
        branches = ['all']
    else:
        # no explicit filter: restrict to the default branches
        branches = DEFAULT_BRANCHES.split()

    if 'all' in branches:
        selected_builders = builders
    else:
        # filter by branch
        selected_builders = [builder for builder in builders
                             if builder.branch in branches]

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

    # Retrieve the last builds
    xrlastbuilds = {}
    if not options.offline:
        for xrb in proxy.getLastBuildsAllBuilders(numbuilds):
            xrlastbuilds.setdefault(xrb[0], []).append(xrb)

    if options.failures:
        print "... retrieving build results"

    counters = dict((s, 0) for s in BUILDER_STATUSES)

    # loop through the builders and their builds
    for builder in selected_builders:
        # If the builder is working, the list may be partial or empty.
        xmlrpcbuilds = xrlastbuilds.get(str(builder), [])

        # Fill the list with tuples like (builder_name, -1).
        lastbuilds = [(str(builder), -1 - i)
                      for i in range(numbuilds - len(xmlrpcbuilds))]
        lastbuilds += reversed(xmlrpcbuilds)

        # default value is True without "-f" option
        found_failure = not options.failures

        if options.offline:
            offset = 1 + builder.lastbuild
        else:
            offset = 0

        builds = []
        for build_info in lastbuilds:
            if offset and build_info[1] < 0:
                build_info = (build_info[0], build_info[1] + offset)
            build = Build(*build_info)

            if not offset and build_info[1] < 0 and build.saved:
                offset = build.num - build_info[1]

            if not found_failure:
                # Retrieve the failed tests
                build.get_message()
                if set(options.failures) <= set(build.failed_tests):
                    found_failure = True

            # These data are accumulated in a list of results which is
            # passed to a printer function.  The same list may be used
            # to generate other kind of reports (e.g. HTML, XML, ...).

            builds.append(build)

        builder.add(*builds)

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

    if options.offline and conn is not None:
        # In offline mode, there's no need to refresh the dump.
        # The database is simply unloaded.
        conn.close()
        conn = None

    print_final(counters)

    return builders


if __name__ == '__main__':
    try:
        # set the builders var -- useful with python -i
        builders = main()
    finally:
        reset_terminal()
        dump_database()
