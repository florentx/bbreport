#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement

import collections
import fnmatch
import gzip
import optparse
import os
import re
import shutil
import socket
import sqlite3
import sys
from contextlib import closing
from datetime import datetime

try:
    import simplejson as json
except ImportError:
    import json

try:
    # Python 2.x
    import urllib2
    import urllib
    import xmlrpclib
    from ConfigParser import ConfigParser
except ImportError:
    # Python 3.x
    import urllib.request as urllib2
    import urllib.parse as urllib
    import xmlrpc.client as xmlrpclib
    from configparser import ConfigParser

try:
    # ANSI color support on Windows
    import colorama
    colorama.init()
except ImportError:
    pass

__version__ = '0.1dev'

# Default number of builds
NUMBUILDS = 4
# The XMLRPC methods may give an error with larger requests
XMLRPC_LIMIT = 5
CACHE_BUILDS = 50
DEFAULT_BRANCHES = 'all'
DEFAULT_FAILURES = ''
DEFAULT_TIMEOUT = 4
MSG_MAXLENGTH = 60
MAX_FAILURES = 30
DEFAULT_OUTPUT = {}
BUILD_ID = 'revision'
ANSI_COLOR = ['black', 'red', 'green', 'yellow',
              'blue', 'magenta', 'cyan', 'white']

baseurl = 'http://www.python.org/dev/buildbot/'
issuesurl = 'http://wiki.bbreport.googlecode.com/hg/KnownIssues.wiki'

# Configuration
basefile = os.path.splitext(__file__)[0]
conffile = basefile + '.conf'
# Database file
dbfile = basefile + '.cache'
# Generated JSON file (option --mode json)
jsonfile = basefile + '.json'

# Database connection
conn = None
# Count removed builds
removed_builds = 0

# Common statuses for Builds and Builders
S_BUILDING = 'building'
S_SUCCESS = 'success'
S_FAILURE = 'failure'
S_EXCEPTION = 'exception'   # Build only (mapped to S_FAILURE)
S_UNSTABLE = 'unstable'     # Builder only (intermittent failures)
S_OFFLINE = 'offline'       # Builder only
S_MISSING = 'missing'       # Builder only

BUILDER_STATUSES = (S_BUILDING, S_SUCCESS, S_UNSTABLE, S_FAILURE, S_OFFLINE)

# bytes/unicode helpers
b = lambda s: s.encode('utf-8')
u = lambda s: s.decode('utf-8')

# Regular expressions
RE_BUILD = re.compile(b('Build #(\d+)</h1>\r?\n?'
                        '<h2>Results:</h2>\r?\n?'
                        '<span class="([^"]+)">([^<]+)</span>'))
RE_BUILD_REVISION = re.compile(b('<li>Revision: (\d+)</li>'))
RE_FAILED = re.compile(b('(\d+) tests? failed:((?:\r?\n? +([^\r\n]+))+)'))
RE_TIMEOUT = re.compile(b('command timed out: (\d+) ([^,]+)'))
RE_STOP = re.compile(b('(process killed by .+)'))
RE_BBTEST = re.compile(b('make: \*\*\* \[buildbottest\] (.+)'))
RE_TEST = re.compile(b('(?:\[[^]]*\] )?(test_[^ <]+)(?:</span>|$)'))

# Buildbot errors
OSERRORS = (b('filesystem is full'),
            b('No space left on device'),
            b('Cannot allocate memory'))

# HTML pollution in the stdio log
HTMLNOISE = b('</span><span class="stdout">')

# Format output
SYMBOL = {S_SUCCESS: '_', S_FAILURE: '#', S_EXCEPTION: '?',
          S_UNSTABLE: '?', S_BUILDING: '*', S_OFFLINE: '*'}

COLOR = {S_SUCCESS: 'green', S_FAILURE: 'red', S_EXCEPTION: 'yellow',
         S_UNSTABLE: 'yellow', S_BUILDING: 'blue', S_OFFLINE: 'cyan'}

_escape_sequence = {}


# ~~ Compatibility with Python 2.5 ~~

if not hasattr(sqlite3.Connection, 'iterdump'):
    try:
        from pysqlite2 import dbapi2 as sqlite3
        sqlite3.Connection.iterdump
    except (ImportError, AttributeError):
        sys.exit("*** Requires pysqlite 2.5.0 or Python >= 2.6")

try:
    from collections import MutableMapping
except ImportError:
    from UserDict import DictMixin as MutableMapping

try:
    next
except NameError:
    def next(iterator, default=None):
        for item in iterator:
            return item
        return default

try:
    out = getattr(__builtins__, 'print')
except AttributeError:
    def out(*args, **kw):
        sys.stdout.write(' '.join(str(arg) for arg in args) +
                         kw.get('end', '\n'))


# ~~ Helpers ~~


def exc():
    return str(sys.exc_info()[1])


def prepare_output():
    # Read the configuration and set the ANSI sequences to colorize the output
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

    for status, color in COLOR.items():
        _escape_sequence[status] = ('%s%s;%sm%%s\x1b[%sm' %
            (_base, fg_offset + ANSI_COLOR.index(color), bg_color, fg_color))

    # Fallback to normal output, without color
    with_color = DEFAULT_OUTPUT.get('color')
    if (with_color is None and not sys.stdout.isatty() or
        with_color in ('false', '0', 'off', 'no')):
        cformat = _cformat_plain


def _cformat_plain(text, status, sep=' '):
    # Straight output: statuses are represented with symbols
    return sep.join((SYMBOL[status], str(text)))


def _cformat_color(text, status, sep=None):
    # Colored output
    return _escape_sequence[status] % text


def reset_terminal():
    if cformat == _cformat_color:
        # Reset terminal colors
        out('\x1b[39;49;00m\r', end='')

cformat = _cformat_color


def trunc(tests, length):
    # Join test names and truncate
    text = ' ' + ' '.join(tests)
    length -= len(text)
    if length < 0:
        text = text[:length - 3] + '...'
    return text, length


def urlread(url):
    # Return an empty string on IOError
    try:
        resource = urllib2.urlopen(url)
        return resource.read()
    except IOError:
        return b('')


def parse_builder_name(name):
    try:
        # the branch name should always be the last part of the name
        host, branch = name.rsplit(None, 1)
    except ValueError:
        host, branch = name, 'unknown'
        if name.endswith('.dmg'):
            # FIXME: fix buildbot names? :-)
            branch = name[:-4]
    return host, branch


# ~~ Builder and Build classes ~~


class Builder(object):
    """Represent a builder."""

    saved = status = None
    lastbuild = 0

    def __init__(self, name):
        self.name = name
        self.host, self.branch = parse_builder_name(name)
        self.url = baseurl + 'builders/' + urllib.quote(name)
        self.builds = {}
        self._load_builder()
        if not self.saved:
            self.save()

    @classmethod
    def query_all(cls):
        """Return the builders from the database, as a dict."""
        if conn is None:
            return {}
        cur = conn.execute('SELECT builder FROM builders WHERE status '
                           'IS NULL OR status <> ?', (S_MISSING,))
        return dict((name, cls(name)) for (name,) in cur.fetchall())

    def get_builds(self, n, *builds):
        """Yield the last n builds.

        Optionally, build tuples can be passed, for builds retrieved by XMLRPC.
        It helps building the list faster, with less server queries.
        """
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
        """Retrieve the last n builds from the local cache."""
        if conn is None:
            return []
        cur = conn.execute('SELECT build FROM builds WHERE builder = ? '
                           'ORDER BY build DESC LIMIT ?', (self.name, n))
        builds = [Build(self.name, num) for (num,) in cur.fetchall()]
        self.add(*builds)
        return builds

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return self.name

    def _load_builder(self):
        """Populate the builder attributes from the local cache."""
        if conn is None:
            return
        row = conn.execute('SELECT lastbuild, status FROM builders WHERE '
                           'builder = ? ', (self.name,)).fetchone()
        if row is not None:
            self.saved = True
            (self.lastbuild, self.status) = row
            if self.status == S_MISSING:
                # Reset the builder status
                self.set_status(None)

    def add(self, *builds):
        """Add a build to this builder, and adjust lastbuild."""
        last = self.lastbuild
        for build in builds:
            self.builds[build.num] = build
            last = max(last, build.num)
        if last > self.lastbuild:
            self.lastbuild = last
            self.remove_oldest()
            self.save()

    def set_status(self, status):
        """Set the builder status."""
        self.status = status
        self.save()

    def remove_oldest(self):
        global removed_builds
        if conn is None:
            return
        if CACHE_BUILDS <= 0:
            return
        # Remove obsolete data
        minbuild = self.lastbuild - CACHE_BUILDS
        cur = conn.execute('DELETE FROM builds WHERE builder = ? AND '
                           'build < ?', (self.name, minbuild))
        if cur.rowcount:
            removed_builds += cur.rowcount

    def save(self):
        """Insert or update the builder in the local cache."""
        if conn is None:
            return
        if self.saved:
            conn.execute('UPDATE builders SET lastbuild = ?, status = ? '
                         'WHERE builder = ?',
                         (self.lastbuild, self.status, self.name))
        else:
            conn.execute('INSERT INTO builders(builder, host, branch, '
                         'lastbuild, status) VALUES (?, ?, ?, ?, ?)',
                         (self.name, self.host, self.branch,
                          self.lastbuild, self.status))
            self.saved = True
        return True


class Build(object):
    """Represent a single build of a builder.

    Build.result should be one of (S_SUCCESS, S_FAILURE, S_EXCEPTION).
    If the result is not available, it defaults to S_BUILDING.
    """
    _message = saved = result = None
    revision = 0

    def __init__(self, name, buildnum, *args):
        self.builder = name
        self.num = buildnum
        self._url = '%s/builders/%s/builds/' % (baseurl, urllib.quote(name))
        self._get_build(args)
        self.failed_tests = []
        if self.result not in (S_SUCCESS, S_BUILDING):
            self._get_failures()
        self.save()

    @property
    def id(self):
        """The build identifier."""
        return getattr(self, BUILD_ID)

    @property
    def url(self):
        """The build URL."""
        return self._url + str(self.num)

    def _get_build(self, args):
        # Load the build data from the cache, or online
        if self.num is not None:
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
        if self._message and self._message.startswith('failed svn'):
            self.result = S_EXCEPTION

    def _get_failures(self):
        # Load the failures from the cache, or parse the stdio log
        if self.saved and conn is not None:
            cur = conn.execute('SELECT failed FROM failures WHERE '
                               'builder = ? AND build = ?',
                               (self.builder, self.num))
            self.failed_tests = [test for (test,) in cur.fetchall()]
        else:
            if self._message is None or 'test' in self._message:
                # Parse stdio on demand
                self._parse_stdio()

    def save(self):
        """Insert the build in the local cache."""
        if conn is None or self.saved:
            return
        if self.result not in (S_SUCCESS, S_FAILURE, S_EXCEPTION):
            return False
        conn.execute('INSERT INTO builds(builder, build, revision, result, '
                     'message) VALUES (?, ?, ?, ?, ?)', (self.builder,
                     self.num, self.revision, self.result, self._message))
        if self.failed_tests:
            rows = ((self.builder, self.num, test)
                    for test in self.failed_tests)
            conn.executemany('INSERT INTO failures(builder, build, failed) '
                             'VALUES (?, ?, ?)', rows)
        self.saved = True
        return True

    def _load_build(self):
        # Load revision, result and message from the local cache
        result = None
        if conn is not None and self.num >= 0:
            row = conn.execute('SELECT revision, result, message FROM builds'
                               ' WHERE builder = ? AND build = ?',
                               (self.builder, self.num)).fetchone()
            if row is not None:
                self.saved = True
                (self.revision, result, self._message) = row
        return result

    def _parse_build(self):
        # Retrieve num, result, revision and message from the server
        build_page = urlread(self.url)
        if not build_page:
            return S_BUILDING
        match = RE_BUILD.search(build_page)
        if match:
            self.num = int(match.group(1))
            result = u(match.group(2))
            self._message = u(match.group(3))
        else:
            result = S_BUILDING
        match = RE_BUILD_REVISION.search(build_page)
        if match:
            self.revision = int(match.group(1))
        self._load_build()
        return result

    def _parse_stdio(self):
        # Lookup failures in the stdio log on the server
        stdio = urlread(self.url + '/steps/test/logs/stdio')
        stdio = stdio.replace(HTMLNOISE, b(''))

        # Check if some test failed
        fail = RE_FAILED.search(stdio)
        if fail:
            failed_count = int(fail.group(1))
            failed_tests = u(fail.group(2).strip())
            self.failed_tests = failed_tests.split()
            assert len(self.failed_tests) == failed_count

        lines = stdio.splitlines()

        # Check if disk full or out of memory
        for line in lines:
            error = next((e for e in OSERRORS if e in line), None)
            if error is None:
                continue
            self.result = S_EXCEPTION
            self._message = u(error.lower())
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
                self._message = u(killed.group(1).strip().lower())
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
                self.failed_tests = [u(failed.group(1))]
                break
        else:
            # No test failure: probably a buildbot error
            self.result = S_EXCEPTION

    def get_message(self, length=2048):
        """Return the build result including failed test as a string."""
        if self.result in (S_SUCCESS, S_BUILDING):
            return cformat(self.result, self.result)
        msg = self._message
        if self.failed_tests:
            failed_tests, known = issues.match(self)
            failed_count = len(failed_tests) + len(known)
            if self.result == S_EXCEPTION and failed_count > 2:
                # disk full or other buildbot error
                msg += ' (%s failed)' % failed_count
            else:
                if not msg:
                    msg = '%s failed' % failed_count
                msg += ':'
                length -= len(msg)
                if failed_tests:
                    (text, length) = trunc(failed_tests, length)
                    msg += cformat(text, S_FAILURE, sep='')
                if known and not (failed_tests and length < 0):
                    msg += trunc(known, length)[0]
        return SYMBOL[self.result] + ' ' + msg


# ~~ Issues ~~


class Rule(tuple):
    """Represent a matching rule for an issue."""

    def __new__(cls, test='', message='', builder=''):
        if not (test or message or builder):
            raise TypeError('A Rule needs a test or a message '
                            'or a builder regex')
        return tuple.__new__(cls, (test, message, builder))

    def __init__(self, test, message, builder):
        # Match the failed test exactly
        if test and not test.endswith('$'):
            test += '$'
        self.test_re = re.compile(test)
        self.message_re = re.compile(message)
        self.builder_re = re.compile(builder)

    def match(self, test, message, builder):
        """Check if the failure attributes match the issue criteria."""
        return all((self.test_re.match(test),
                    self.message_re.match(message),
                    self.builder_re.match(builder)))


class MatchIssue(object):
    """Represent an issue from the issue tracker."""

    def __init__(self, number, *rules):
        self.number = number
        self.rules = [Rule(*rule) for rule in rules]
        self.events = {}

    def __str__(self):
        lines = []
        out = lines.append

        for rule in self.rules:
            out('%s: %s:%s:%s' % ((self.number,) + rule))
        indent = ' ' * (len(self.number) + 2)
        for failure, builds in sorted(self.events.items()):
            out(indent + ':'.join(failure) + ' ' +
                cformat(' '.join(str(b.id) for b in builds), S_UNSTABLE))

        return '\n'.join(lines)

    def add(self, rule):
        rule = Rule(*rule)
        if rule not in self.rules:
            self.rules.append(rule)

    def match(self, build, *event):
        """Check if the failure attributes match any issue criteria."""
        rv = any(rule.match(*event) for rule in self.rules)
        if rv:
            self.events.setdefault(event, []).append(build)
        return rv


class Issues(dict, MutableMapping):
    """Ordered dictionary of issues from the issue tracker."""

    def __init__(self, *args, **kw):
        self.__keys = []
        self._preload = []
        self.new_events = {}
        self.update(*args, **kw)
        # By default do not record
        self.__record = False

    def __setitem__(self, key, value):
        if key in self:
            self[key].add(value)
        else:
            self.__keys.append(key)
            dict.__setitem__(self, key, MatchIssue(key, value))
        if self.__record:
            conn.execute('INSERT INTO rules(issue, test, message, builder) '
                         'VALUES (?, ?, ?, ?)', (key,) + tuple(value))

    def __iter__(self):
        return iter(self.__keys)

    try:
        items = MutableMapping.iteritems
    except AttributeError:
        # Python 3
        items = MutableMapping.items

    def values(self):
        """Return the issues by number of events descending."""
        return sorted(dict.values(self), key=lambda m: -len(m.events))

    def clear(self, record=True):
        del self.__keys[:]
        self.new_events.clear()
        dict.clear(self)
        if conn is not None:
            # Clear all entries before recording
            conn.execute('DELETE FROM rules')
            self.__record = record

    def load(self, offline=False):
        """Populate the issues."""
        if not offline:
            page = urlread(issuesurl)
            if page:
                # Reset the table
                self.clear()
            else:
                # If page is empty, use cache
                offline = True
        if offline:
            # Load the cache first
            self._load_from_cache()
        if self._preload:
            # Load local configuration
            for issue, rule in self._preload:
                self[issue] = rule
            del self._preload[:]
        if not offline:
            # Load online issues
            self._load_from_page(u(page))

    def _load_from_cache(self):
        """Load the issues from the local cache."""
        if conn is None:
            return
        cur = conn.execute('SELECT issue, test, message, builder FROM rules')
        for row in cur.fetchall():
            self[row[0]] = row[1:]
        # Allow recording only if table is empty
        self.__record = not cur.rowcount

    def _load_from_page(self, page):
        """Retrieve the issues from the page."""
        for line in page.splitlines():
            if not line.startswith('||'):
                continue
            # Split table cells
            cells = line.split('||')[1:-1]
            if len(cells) < 4:
                # Skip incomplete rules
                continue
            # Strip backquotes
            rule = [cell.strip(' \t`') for cell in cells]
            # Skip headers (bold formatted)
            if rule[0][0] != '*':
                self[rule[0]] = rule[1:4]

    def match(self, build):
        msg = build._message
        builder = build.builder
        known = []
        new = []
        new_events = self.new_events
        for test in build.failed_tests:
            event = (test, msg, builder)
            issue = next((number for number, issue in self.items()
                          if issue.match(build, test, msg, builder)), False)
            if issue:
                test += '`%s' % issue
                known.append(test)
            else:
                new.append(test)
                new_events.setdefault(event, []).append(build)
        return new, known

    def new_failures(self, verbose=False):
        lines = []
        out = lines.append

        new_failures = self.new_events
        if new_failures:
            count = len(new_failures)
            if verbose or count <= MAX_FAILURES:
                out('\n%s new test failure(s):' % count)
                for failure, builds in sorted(new_failures.items()):
                    out('     ' + ':'.join(failure) + ' ' +
                        cformat(' '.join(str(b.id) for b in builds),
                                S_FAILURE))
            else:
                out('  and ' +
                    cformat('%s new test failures' % count, S_FAILURE))

        return '\n'.join(lines)

    def __str__(self):
        return ('\n'.join(str(issue) for issue in self.values()) + '\n' +
                self.new_failures(verbose=True))

# Instanciate a global Issues dictionary
issues = Issues()


# ~~ Output classes ~~


class AbstractOutput(object):
    """Base class for output."""

    def __init__(self, options):
        self.options = options

    def add_builds(self, name, builds):
        """Add builds for a builder.

        This method adds builds to the output object.
        It can render a message after each addition.

        Arguments:
          - name: builder name (str)
          - builds: list of Build objects
        """
        pass

    def display(self):
        """Display result.

        This method is called once, after all builds have been added to
        the output object.  It renders the final message.
        """
        pass


class BuilderOutput(AbstractOutput):
    """Default output."""

    def __init__(self, options):
        AbstractOutput.__init__(self, options)
        self.counters = dict((s, 0) for s in BUILDER_STATUSES)
        self.groups = dict((s, []) for s in BUILDER_STATUSES)

    def print_builder(self, name, builds):
        """Print the builder result."""
        quiet = self.options.quiet

        count = {S_SUCCESS: 0, S_FAILURE: 0}
        capsule = []
        failed_builds = []
        display_builds = []

        for build in builds:
            # Save horizontal space, printing only the last 3 digits
            compact = (quiet or len(builds) > 6) and len(capsule) > 1
            if build is None:
                if len(capsule) < NUMBUILDS:
                    capsule.append(' ' * (5 if not compact else 3))
                continue

            result = build.result

            if build.id:
                id = '%5d' % build.id
                id = id if not compact else id[-3:]
            else:
                id = ' *** ' if not compact else '***'
            capsule.append(cformat(id, result, sep=''))

            if result == S_BUILDING:
                continue
            elif result == S_SUCCESS:
                count[S_SUCCESS] += 1
                if self.options.verbose:
                    display_builds.append(build)
            else:
                count[S_FAILURE] += 1
                failed_builds.append(build)
                display_builds.append(build)

        is_active = ((builds[0] and builds[0].revision) or
                     count[S_SUCCESS] > 0 or count[S_FAILURE] > 0)

        if quiet > 1:
            # Print only the colored buildbot names
            if not is_active:
                return S_OFFLINE
            last_result = builds[0].result
            if last_result in (S_SUCCESS, S_BUILDING):
                return last_result
            return S_FAILURE

        if count[S_SUCCESS] == 0:
            if is_active:
                builder_status = S_FAILURE
            else:
                builder_status = S_OFFLINE
                capsule = [cformat(' *** ', S_OFFLINE, sep='')] * 2
        elif count[S_FAILURE] > 0:
            builder_status = S_UNSTABLE
        else:
            builder_status = S_SUCCESS

        out(cformat('%-26s' % name, builder_status), ', '.join(capsule),
            end=' ')

        if quiet and failed_builds:
            # Print last failure or error.
            out(failed_builds[0].get_message(MSG_MAXLENGTH))
        else:
            # Move to next line
            out()

        if not quiet:
            for build in display_builds:
                out('%4d %5d:' % (build.num, build.revision),
                    build.get_message())

        return builder_status

    def add_builds(self, name, builds):
        builder_status = self.print_builder(name, builds)

        if self.options.quiet > 1:
            self.groups[builder_status].append(name)

        self.counters[builder_status] += 1

    def display(self):
        totals = []
        for status in BUILDER_STATUSES:
            if self.counters[status]:
                totals.append(cformat(self.counters[status], status, sep=':'))

        # With -qq option
        if self.options.quiet > 1:
            self._group_by_status()

        # Show the summary at the bottom
        out('Totals:', ' + '.join(totals))
        out(issues.new_failures())

    def _group_by_status(self):
        for status in BUILDER_STATUSES:
            names = self.groups[status]
            if not names:
                continue
            platforms = {}
            for name in names:
                try:
                    host, branch = name.rsplit(None, 1)
                except ValueError:
                    host, branch = name, ''
                platforms.setdefault(host, []).append(branch)

            out(cformat(status.title() + ':', status))
            for host, branches in sorted(platforms.items()):
                out('\t' + cformat(host, status), ', '.join(branches))


class Branch(object):
    """Represent all results of a specific branch.

    Used for the RevisionOutput.
    """

    def __init__(self, name):
        self.name = name
        self.revisions = {}
        self.last_revision = 0


class Revision(object):
    """Represent all results for a revision.

    Used for the RevisionOutput.
    """

    def __init__(self, number):
        self.number = number
        self.by_status = collections.defaultdict(list)


class RevisionOutput(AbstractOutput):
    """Alternative output by revision."""

    def __init__(self, options):
        AbstractOutput.__init__(self, options)
        self.branches = {}
        out("... retrieving build results")

    def add_builds(self, name, builds):
        host, branch_name = parse_builder_name(name)
        for build in builds:
            if build is None or build.revision == 0:
                continue
            try:
                branch = self.branches[branch_name]
            except KeyError:
                branch = Branch(branch_name)
                self.branches[branch.name] = branch
            branch.last_revision = max(branch.last_revision, build.revision)
            text = self.format_build(build)
            if text is None:
                continue
            try:
                revision = branch.revisions[build.revision]
            except KeyError:
                revision = Revision(build.revision)
                branch.revisions[build.revision] = revision
            revision.by_status[build.result].append(text)

        # Filter revisions: remove success and building builds
        # depending on verbose and quiet options
        for branch in self.branches.values():
            branch_items = list(branch.revisions.items())
            for number, revision in branch_items:
                results = list(revision.by_status.keys())
                for result in results:
                    if not self.options.verbose and (result == S_BUILDING or
                       (result == S_SUCCESS and (self.options.quiet > 1 or
                       revision.number != branch.last_revision))):
                        del revision.by_status[result]
                if not revision.by_status:
                    del branch.revisions[number]

    def format_build(self, build):
        msg = build.builder
        length = 2048
        if build.result not in (S_SUCCESS, S_BUILDING):
            if build.result == S_EXCEPTION and (not self.options.verbose):
                # Hide exceptions
                return None
            build_message = build._message
            if build.failed_tests:
                new_events, known = issues.match(build)
                if new_events:
                    (text, length) = trunc(new_events, length)
                    msg += ':' + cformat(text, S_FAILURE, sep='')
                elif self.options.quiet:
                    # Hide known failures
                    return None
                if known:
                    msg += trunc(known, length)[0]
            else:
                msg += ': "%s"' % build_message
        else:
            msg = cformat(msg, build.result)
        return msg

    def display(self):
        display_name = (len(self.branches) != 1)
        empty_line = False
        for branch in self.branches.values():
            if display_name:
                if empty_line:
                    out()
                title = "Branch %s" % branch.name
                out(title)
                out("=" * len(title))
                out()
            self.display_revisions(branch.revisions)
            empty_line = True

    def display_revisions(self, revisions):
        revisions = sorted(revisions.items())
        for number, revision in revisions:
            out("r%s:" % number)
            for result, builds in revision.by_status.items():
                for text in builds:
                    out(' ' + text)


class IssueOutput(AbstractOutput):
    """Alternative output by issue."""

    def __init__(self, options):
        AbstractOutput.__init__(self, options)
        out("... retrieving build results")
        self.broken = {}
        self.count_build = options.limit or NUMBUILDS

    def add_builds(self, name, builds):
        """Add builds for a builder."""
        broken = True
        messages = []
        for build in filter(None, builds):
            if build.result == S_SUCCESS:
                broken = False
            elif build.failed_tests:
                # Load the build results
                build.get_message()
                broken = False
            elif broken and build.result != S_BUILDING:
                messages.append(build.get_message())
        if broken:
            if not messages:
                messages.append(SYMBOL[S_OFFLINE] + ' ' + S_OFFLINE)
            try:
                host, branch = name.rsplit(None, 1)
            except ValueError:
                host, branch = name, ''
            host = self.broken.setdefault(host, {'branches': [],
                                                 'messages': []})
            host['branches'].append(branch)
            for msg in messages:
                if msg not in host['messages']:
                    host['messages'].append(msg)

    def display(self):
        """Display result."""
        # Known issues and new failures
        out(issues)
        out()

        # Broken builders
        self.print_broken_builders()

    def print_broken_builders(self):
        """Print broken and offline builders."""
        out('Broken builders:')
        for host, builder in sorted(self.broken.items()):
            branches = cformat(' '.join(builder['branches']), S_OFFLINE)
            messages = ', '.join(builder['messages'])
            out('\t' + host, branches, messages)


class JsonOutput(IssueOutput):
    """JSON output, subclass of IssueOutput."""

    def display(self):
        """Display result."""

        def format_failure(failure, builds):
            test, message, builder = failure
            return {
                'test': test,
                'message': message,
                'builder': builder,
                'builds': [(b.num, b.revision) for b in builds],
            }

        # Known issues
        known = []
        gone = []
        for issue in issues.values():
            rv = {
                'issue': issue.number,
                'rules': [{'test': test, 'message': msg, 'builder': builder}
                          for test, msg, builder in issue.rules],
            }
            if issue.events:
                rv['failures'] = [format_failure(*f)
                                  for f in sorted(issue.events.items())]
                known.append(rv)
            else:
                gone.append(rv)

        # New failures
        new_failures = issues.new_events
        count_new = len(new_failures)
        new = [format_failure(*f) for f in sorted(new_failures.items())]

        # Broken builders
        broken = [{
            'host': host,
            'branches': builder['branches'],
            'messages': builder['messages'],
        } for host, builder in sorted(self.broken.items())]

        with open(jsonfile, 'w') as f:
            json.dump({
                'count_build': self.count_build,
                'count_new': count_new,
                'changed': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
                'new': new,
                'known': known,
                'gone': gone,
                'broken': broken,
            }, f, indent=1, separators=(',', ': '))


# ~~ Local cache ~~


def load_database():
    global conn
    if conn is None:
        conn = sqlite3.connect(':memory:')
    if os.path.exists(dbfile):
        # Load the database in memory
        with closing(gzip.open(dbfile, 'rb')) as f:
            conn.executescript(u(f.read()))
    # Initialize or upgrade the tables
    for table in ('builders(builder, host, branch, lastbuild, status)',
                  'builds(builder, build, revision, result, message)',
                  'failures(builder, build, failed)',
                  'rules(issue, test, message, builder)'):
        conn.execute('CREATE TABLE IF NOT EXISTS ' + table)


def prune_database():
    if removed_builds:
        out('Removed %s ancient builds' % removed_builds)
        # Now purge the failures
        conn.execute('DELETE FROM failures WHERE builder||":"||build '
                     'NOT IN (SELECT builder||":"||build FROM builds)')


def dump_database():
    # Backup previous dump (and overwrite existing backup)
    if os.path.exists(dbfile):
        shutil.move(dbfile, dbfile + '.bak')
    # Dump the database
    with closing(gzip.open(dbfile, 'wb')) as f:
        f.writelines(b(l + os.linesep) for l in conn.iterdump())


# ~~ Application configuration ~~


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
                      help='the Python branches (e.g. 2.7,3.x)')
    parser.add_option('-u', '--build', dest='build', default=None,
                      metavar='num', help='the build number of a buildslave'
                                          ' (not implemented)')
    parser.add_option('-f', '--failures', dest='failures',
                      action='append', default=[],
                      metavar='test_xyz', help='the name of a failed test')
    parser.add_option('-l', '--limit', default=0, type="int",
                      help='limit the number of builds per builder '
                           '(default: %s)' % NUMBUILDS)
    parser.add_option('-r', '--revision',
                      help='minimum revision number',
                      type='int', default=None)
    parser.add_option('-v', '--verbose', default=0, action='count',
                      help='display also success')
    parser.add_option('-q', '--quiet', default=0, action='count',
                      help='one line per builder, or group by status with -qq')
    parser.add_option('-o', '--offline', default=False, action='store_true',
                      help='use only the local database; no update')
    parser.add_option('--no-color', default=False, action='store_true',
                      help='do not color the output')
    parser.add_option('--no-database', default=False, action='store_true',
                      help='do not cache the result in a database file')
    parser.add_option('--mode', default="builder", type="choice",
                      choices=("builder", "revision", "issue", "json"),
                      help='output mode: "builder", "revision" or "issue"')
    parser.add_option('--id', default="revision", type="choice",
                      choices=("revision", "build"),
                      help='build identifier: "revision" or "build"')
    parser.add_option('--conf', default=conffile,
                      metavar='FILE', help='configuration file')

    options, args = parser.parse_args()

    if options.offline and options.no_database:
        out("--offline and --no-database don't go together")
        sys.exit(1)

    return options, args


def configure():
    global BUILD_ID, cformat

    # Parse command line arguments
    options, args = parse_args()

    # Load the configuration from the file
    conf = ConfigParser()
    conf.read(options.conf)
    sections = conf.sections()
    if 'global' in sections:
        glow = dict((k.lower(), k) for k in globals())
        for k, v in conf.items('global'):
            key = glow.get(k.lower())
            if key:
                conv = type(globals()[key])  # int or str
                globals()[key] = conv(v)
    if 'output' in sections:
        DEFAULT_OUTPUT.update(conf.items('output'))
    if 'colors' in sections:
        COLOR.update(conf.items('colors'))
    if 'symbols' in sections:
        SYMBOL.update(conf.items('symbols'))
    if 'issues' in sections:
        # Preload the known issues
        for num, val in conf.items('issues'):
            rule = tuple(arg.strip() for arg in val.split(':'))
            issues._preload.append((num, rule))

    # Set timeout
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)

    # Prepare the output colors
    prepare_output()

    # Tweak configuration

    if DEFAULT_FAILURES and not options.failures:
        options.failures = DEFAULT_FAILURES.split()

    if options.failures:
        # ignore the -q option
        options.quiet = 0

    if options.no_color:
        # replace the colorizer
        cformat = _cformat_plain

    if options.id == "build":
        # Use the build number as identifier
        BUILD_ID = "num"

    # out(options, args)
    return options, args


# ~~ Main function ~~


def main():
    global conn

    # Load configuration
    options, args = configure()

    if not options.no_database:
        try:
            # Load the database
            load_database()
        except Exception:
            conn = None

    # Load issues (online or from cache)
    issues.load(offline=options.offline)

    builders = Builder.query_all()
    if not options.offline:
        # create the xmlrpc proxy to retrieve the build data
        proxy = xmlrpclib.ServerProxy(baseurl + 'all/xmlrpc')

        # create the list of builders
        try:
            current_builders = set(proxy.getAllBuilders())
        except socket.error:
            # Network is unreachable
            out('***', exc() + ', unable to refresh the list of builders')
            current_builders = None

        # Do nothing if the RPC call returns an empty set
        if current_builders:

            saved_builders = set(builders.keys())
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
    out('Selected builders:', len(selected_builders), '/', len(builders),
        '(branch%s: %s)' % ('es' if len(branches) > 1 else '',
                            ', '.join(branches)))

    if options.quiet > 1:
        # For the "-qq" option, 2 builds per builder is enough
        numbuilds = 2
        out("... retrieving last build results")
    elif options.quiet or options.limit or len(selected_builders) > 2:
        numbuilds = options.limit or NUMBUILDS
    else:
        # show more builds
        numbuilds = NUMBUILDS * 2

    # Retrieve the last builds
    xrlastbuilds = {}
    if not options.offline:
        # don't overload the server with huge requests.
        limit = min(XMLRPC_LIMIT, numbuilds)
        try:
            for xrb in proxy.getLastBuildsAllBuilders(limit):
                xrlastbuilds.setdefault(xrb[0], []).append(xrb)
        except xmlrpclib.Error:
            out('*** xmlrpclib.Error:', exc())
        except socket.error:
            # Network is unreachable
            out('***', exc() + ', unable to retrieve the last builds')
            if not options.no_database:
                out('*** running in offline mode')
                options.offline = True

    if options.failures:
        out("... retrieving build results")

    # loop through the builders and their builds
    if options.mode == "revision":
        output_class = RevisionOutput
    elif options.mode == "issue":
        output_class = IssueOutput
    elif options.mode == "json":
        output_class = JsonOutput
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

        # filter by revision number
        if options.revision:
            builds = [b for b in builds if b.revision >= options.revision]

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

    if not options.offline and conn is not None:
        prune_database()
        dump_database()

    return builders


if __name__ == '__main__':
    try:
        # set the builders var -- useful with python -i
        builders = main()
    finally:
        reset_terminal()
