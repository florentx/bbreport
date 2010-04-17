#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib
import urllib2
import fnmatch
import gzip
import optparse
import os
import shutil
import sqlite3
import sys
import xmlrpclib
import collections
from ConfigParser import ConfigParser

__version__ = '0.1dev'

NUMBUILDS = 6
DEFAULT_BRANCHES = 'all'
DEFAULT_TIMEOUT = 2
MSG_MAXLENGTH = 60
DEFAULT_OUTPUT = {}
ANSI_COLOR = ('black', 'red', 'green', 'yellow',
              'blue', 'magenta', 'cyan', 'white')

baseurl = 'http://www.python.org/dev/buildbot/'

# Configuration
basefile = os.path.splitext(__file__)[0]
conffile = basefile + '.conf'
# Database file
dbfile = basefile + '.cache'
# Old location (deprecated)
legacy_dbfile = basefile + '.sqlite'

# Database connection
conn = None
# Known issues
issues = []

# Common statuses for Builds and Builders
S_BUILDING = 'building'
S_SUCCESS = 'success'
S_FAILURE = 'failure'
S_EXCEPTION = 'exception'   # Build only (mapped to S_FAILURE)
S_UNSTABLE = 'unstable'     # Builder only (intermittent failures)
S_OFFLINE = 'offline'       # Builder only
S_MISSING = 'missing'       # Builder only

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

_escape_sequence = {}
_colors = {S_SUCCESS: 'green', S_FAILURE: 'red', S_EXCEPTION: 'yellow',
           S_UNSTABLE: 'yellow', S_BUILDING: 'blue', S_OFFLINE: 'black'}


def prepare_output():
    global cformat

    default_fg = DEFAULT_OUTPUT.get('foreground', '').lower()
    default_bg = DEFAULT_OUTPUT.get('background', '').lower()
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

    if not sys.stdout.isatty() or (str(DEFAULT_OUTPUT.get('color')).lower() in
                                   ('false', '0', 'off', 'no')):
        cformat = _cformat_plain


def _cformat_plain(text, color, sep=' '):
    return sep.join((SYMBOL[color], str(text)))


def _cformat_color(text, color, sep=None):
    return _escape_sequence[color] % text


def reset_terminal():
    if cformat == _cformat_color:
        # reset terminal colors
        print '\x1b[39;49;00m',
    print

cformat = _cformat_color


def trunc(tests, length):
    # Join test names and truncate
    text = ' ' + ' '.join(tests)
    length -= len(text)
    if length < 0:
        text = text[:length -3] + '...'
    return text, length


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
        Alternate constructor to create the object from a serialized builder.
        """
        pass

    @classmethod
    def query_all(cls):
        # Return the builders from the database, as a dict
        if conn is None:
            return []
        cur = conn.execute('select builder from builders where status '
                           'is null or status <> ?', (S_MISSING,))
        return dict((name, cls(name)) for (name,) in cur.fetchall())

    def get_builds(self, n, *builds):
        if builds:
            # The list is not empty.  Maybe the first build is missing.
            if len(builds) < n:
                last = Build(self.name, -1)
                if last.num != builds[-1][1]:
                    self.add(last)
                    yield last
                    n -= 1
            for build_info in reversed(builds):
                build = Build(*build_info)
                self.add(build)
                yield build
            if build.num == 0:
                return
            n -= len(builds)
            offset = build.num - 1
        else:
            # The list is empty.  Retrieve the builds by number (-1, -2, ...).
            offset = -1
        for i in range(n):
            num = offset - i
            build = Build(self.name, num)
            if offset < 0 < build.num:
                # use the real build numbers
                offset = build.num + i
            self.add(build)
            yield build
            # Reach the build #0? stop
            if num == 0:
                return

    def get_saved_builds(self, n):
        if conn is None:
            return []
        cur = conn.execute('select build from builds where builder = ? '
                           'order by build desc limit ?', (self.name, n))
        builds = [Build(self.name, num) for (num,) in cur.fetchall()]
        self.add(*builds)
        return builds

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

    def set_status(self, status):
        self.status = status
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


class MatchIssue(object):

    def __init__(self, number, test='', message='', builder=''):
        if not (test or message or builder):
            raise Exception('MatchIssue needs a test or a message '
                            'or a builder regex')
        self.number = number
        # Match the failed test exactly
        if test and not test.endswith('$'):
            test += '$'
        self.test = re.compile(test)
        self.message = re.compile(message)
        self.builder = re.compile(builder)

    def match(self, test, message, builder):
        return all((self.test.match(test),
                    self.message.match(message),
                    self.builder.match(builder)))


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
        self._get_build(buildnum, args)
        self.failed_tests = []
        if self.result not in (S_SUCCESS, S_BUILDING):
            self._get_failures()
        self.save()

    def _get_build(self, buildnum, args):
        if buildnum is not None:
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

    def _get_failures(self):
        if self.saved and conn is not None:
            cur = conn.execute('select failed from failures where '
                               'builder = ? and build = ?',
                               (self.builder, self.num))
            self.failed_tests = [test for (test,) in cur.fetchall()]
        else:
            if self._message is None or 'test' in self._message:
                # Parse stdio on demand
                self._parse_stdio()

    @classmethod
    def fromdump(cls, data):
        """
        Alternate constructor to create the object from a serialized builder.
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
            rows = ((self.builder, self.num, test)
                    for test in self.failed_tests)
            conn.executemany('insert into failures(builder, build, failed) '
                             'values (?, ?, ?)', rows)
        self.saved = True
        return True

    def _load_build(self):
        # retrieve revision, result and message
        result = None
        if conn is not None and self.num >= 0:
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

    def get_message(self, length=2048):
        if self.result in (S_SUCCESS, S_BUILDING):
            return self.result
        msg = self._message
        if self.failed_tests:
            failed_tests = []
            known = []
            for test in self.failed_tests:
                issue = next((issue for issue in issues
                              if issue.match(test, msg, self.builder)), None)
                if issue:
                    test += '`%s' % issue.number
                    known.append(test)
                else:
                    failed_tests.append(test)
            failed_count = len(failed_tests) + len(known)
            if self.result == S_EXCEPTION and failed_count > 2:
                # disk full or other buildbot error
                msg += ' (%s failed)' % failed_count
            else:
                if not msg:
                    msg = '%s failed' % failed_count
                length -= len(msg)
                if failed_tests:
                    (text, length) = trunc(failed_tests, length)
                    msg += cformat(text, S_FAILURE, sep='')
                if known and not (failed_tests and length < 0):
                    msg += trunc(known, length)[0]
        return SYMBOL[self.result] + ' ' + msg

    def asdict(self):
        """
        Convert the object in an easy-serializable dict.
        """
        return dict(num=self.num, data=self.data)


def load_configuration():
    global issues

    conf = ConfigParser()
    conf.read(conffile)
    sections = conf.sections()
    if 'global' in sections:
        glow = dict((k.lower(), k) for k in globals())
        for k, v in conf.items('global'):
            key = glow.get(k.lower())
            if key:
                globals()[key] = v
    if 'output' in sections:
        DEFAULT_OUTPUT.update(conf.items('output'))
    # Prepare the output colors
    prepare_output()
    if 'issues' not in sections:
        return
    # Load the known issues
    for num, rule in conf.items('issues'):
        args = (arg.strip() for arg in rule.split(':'))
        issues.append(MatchIssue(num, *args))


def upgrade_dbfile():
    # Now the database file is gzipped
    if os.path.exists(legacy_dbfile) and not os.path.exists(dbfile):
        out = gzip.open(dbfile, 'wb')
        try:
            with open(legacy_dbfile, 'rb') as in_:
                out.write(in_.read())
        finally:
            out.close()
        os.unlink(legacy_dbfile)


def load_database():
    # Upgrade the database file format
    upgrade_dbfile()
    global conn
    if conn is None:
        conn = sqlite3.connect(':memory:')
    if os.path.exists(dbfile):
        f = gzip.open(dbfile, 'rb')
        try:
            conn.executescript(f.read())
        finally:
            f.close()
    else:
        # Initialize the tables
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
    f = gzip.open(dbfile, 'wb')
    try:
        f.writelines(l + os.linesep for l in conn.iterdump())
    finally:
        f.close()
    # Close the connection
    conn.close()


class AbstractOutput(object):
    def __init__(self, options):
        self.options = options

    def add_builds(self, name, builds):
        # name: builder name (str)
        # builds: list of Build objects
        pass

    def display(self):
        pass

    def display_final(self):
        pass


class BuilderOutput(AbstractOutput):
    def __init__(self, options):
        AbstractOutput.__init__(self, options)
        self.counters = dict((s, 0) for s in BUILDER_STATUSES)
        self.groups = dict((s, []) for s in BUILDER_STATUSES)

    def print_builder(self, name, builds):
        quiet = self.options.quiet

        count = {S_SUCCESS: 0, S_FAILURE: 0}
        capsule = []
        failed_builds = []

        for build in builds:
            compact = (quiet or len(builds) > 6) and len(capsule) > 1
            if build is None:
                if len(capsule) < NUMBUILDS:
                    capsule.append(' ' * (5 if not compact else 3))
                continue

            result = build.result

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
            print failed_builds[0].get_message(MSG_MAXLENGTH)
        else:
            # Move to next line
            print

        if not quiet:
            for build in failed_builds:
                print ' %5d:' % build.revision, build.get_message()

        return builder_status

    def add_builds(self, name, builds):
        builder_status = self.print_builder(name, builds)

        if self.options.quiet > 1:
            self.groups[builder_status].append(name)

        self.counters[builder_status] += 1

    def display(self):
        if self.options.quiet <= 1:
            return
        for status in BUILDER_STATUSES:
            names = self.groups[status]
            if not names:
                continue
            platforms = {}
            for name in names:
                host, branch = name.rsplit(None, 1)
                platforms.setdefault(host, []).append(branch)

            print cformat(status.title() + ':', status)
            for host, branches in sorted(platforms.items()):
                print '\t' + cformat(host, status), ', '.join(branches)

    def display_final(self):
        totals = []
        for status in BUILDER_STATUSES:
            if self.counters[status]:
                totals.append(cformat(self.counters[status], status, sep=':'))
        print 'Totals:', ' + '.join(totals),


class Revision:
    def __init__(self):
        self.by_status = collections.defaultdict(list)


class RevisionOutput(AbstractOutput):
    def __init__(self, options):
        AbstractOutput.__init__(self, options)
        self.revisions = {}

    def add_builds(self, name, builds):
        for build in builds:
            if build is None:
                continue
            if build.revision == 0:
                continue
            try:
                revision = self.revisions[build.revision]
            except KeyError:
                revision = Revision()
                self.revisions[build.revision] = revision
            revision.by_status[build.result].append(name)

    def display(self):
        revisions = self.revisions.items()
        revisions.sort(key=lambda (key, value): key)
        for number, revision in revisions:
            results = ', '.join(
                '%s={%s}' % (cformat(key, key), ', '.join(values))
                for key, values in revision.by_status.iteritems())
            print "%s: %s" % (number, results)

    def display_final(self):
        pass


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
                      help='the Python branches (e.g. trunk,3.1,3.x)')
    parser.add_option('-u', '--build', dest='build', default=None,
                      metavar='num', help='the build number of a buildslave'
                                          ' (not implemented)')
    parser.add_option('-f', '--failures', dest='failures',
                      action='append', default=[],
                      metavar='test_xyz', help='the name of a failed test')
    parser.add_option('-l', '--limit', default=0, type="int",
                      help='limit the number of builds per builder '
                           '(default: %s)' % NUMBUILDS)
    parser.add_option('-q', '--quiet', default=0, action='count',
                      help='one line per builder, or group by status with -qq')
    parser.add_option('-o', '--offline', default=False, action='store_true',
                      help='use only the local database; no update')
    parser.add_option('--no-color', default=False, action='store_true',
                      help='do not color the output')
    parser.add_option('--no-database', default=False, action='store_true',
                      help='do not cache the result in a database file')
    parser.add_option('--mode', default="builder", type="choice",
                      choices=("builder", "revision"),
                      help='output mode: "builder" or "revision"')

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

    load_configuration()
    options, args = parse_args()

    if not options.no_database:
        # Load the database
        load_database()

    builders = Builder.query_all()
    if not options.offline:
        # create the xmlrpc proxy to retrieve the build data
        proxy = xmlrpclib.ServerProxy(baseurl + 'all/xmlrpc')

        # create the list of builders
        # XXX: add a timeout
        current_builders = set(proxy.getAllBuilders())
        saved_builders = set(builders.keys())

        # Do nothing if the RPC call returns an empty set
        if current_builders:

            missing_builders = saved_builders - current_builders
            added_builders = current_builders - saved_builders

            # flag the obsolete builders
            for name in missing_builders:
                builders.pop(name).set_status(S_MISSING)

            # refresh the dict of builders
            for name in added_builders:
                builders[name] = Builder(name)

    # sort by branch and name
    builders = sorted(builders.values(), key=lambda b: (b.branch, str(b)))

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

    branches = sorted(set(b.branch for b in selected_builders))
    print 'Selected builders:', len(selected_builders), '/', len(builders),
    print '(branch%s: %s)' % ('es' if len(branches) > 1 else '',
                              ', '.join(branches))

    if options.quiet > 1:
        # For the "-qq" option, 2 builds per builder is enough
        numbuilds = 2
        print "... retrieving last build results"
    elif options.quiet or options.limit or len(selected_builders) > 2:
        numbuilds = options.limit or NUMBUILDS
    else:
        # show more builds
        numbuilds = NUMBUILDS * 2

    # Retrieve the last builds
    xrlastbuilds = {}
    if not options.offline:
        # don't overload the server with huge requests.
        limit = min(NUMBUILDS * 2, numbuilds)
        for xrb in proxy.getLastBuildsAllBuilders(limit):
            xrlastbuilds.setdefault(xrb[0], []).append(xrb)

    if options.failures:
        print "... retrieving build results"

    # loop through the builders and their builds
    if options.mode == "revision":
        output_class = RevisionOutput
    else:
        output_class = BuilderOutput
    output = output_class(options)
    for builder in selected_builders:

        # These data are accumulated in a list of results which is
        # passed to a printer function.  The same list may be used
        # to generate other kind of reports (e.g. HTML, XML, ...).

        if options.offline:
            # Read the cached builds
            builds = builder.get_saved_builds(numbuilds)
        else:
            # If the builder is working, the list may be partial or empty.
            xmlrpcbuilds = xrlastbuilds.get(str(builder), [])

            builds = list(builder.get_builds(numbuilds, *xmlrpcbuilds))

        # fill the build list with None for missing builds.
        builds.extend([None] * (numbuilds - len(builds)))

        if (options.failures and
            not any(build is not None and build.failed_tests and
                    set(options.failures) <= set(build.failed_tests)
                    for build in builds)):
            # no build matched the options.failures
            continue

        output.add_builds(str(builder), builds)

    output.display()

    if options.offline and conn is not None:
        # In offline mode, there's no need to refresh the dump.
        # The database is simply unloaded.
        conn.close()
        conn = None

    output.display_final()

    return conn, builders


if __name__ == '__main__':
    try:
        # set some global vars -- useful with python -i
        (conn, builders) = main()
    finally:
        reset_terminal()
        dump_database()
