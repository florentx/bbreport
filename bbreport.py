# -*- coding: utf-8 -*-
import re
import xml
import json
import urllib
import fnmatch
import optparse
import xmlrpclib
from BeautifulSoup import BeautifulSoup

baseurl = 'http://www.python.org/dev/buildbot/'

class Builder(object):
    """
    Represent a builder.
    """
    def __init__(self, name):
        self.name = name
        # the branch name should always be the last part of the name
        self.branch = name.split()[-1]
        self.url = urllib.quote(baseurl + 'builders/' + name)
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
    """
    def __init__(self, buildnum):
        self.num = buildnum
        self.data = None
        self.result = None
        self.failed_tests = []

    @classmethod
    def fromdump(self, data):
        """
        Alternate contructor to create the object from a serialized builder.
        """
        pass

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if data is None:
            self._data = None
            return
        if data['full_error']:
            match = re.search('\d+ tests? failed:\r?\n?([^\r\n]+)',
                            data['full_error']['test'])
            if match:
                failed_tests = match.group(1).strip()
                self.result = 'failed tests: ' + failed_tests
                self.failed_tests = failed_tests.split()
            else:
                self.result = 'failed tests: something crashed'
        else:
            self.result = 'success'

    def asdict(self):
        """
        Convert the object in an easy-serializable dict.
        """
        return dict(num=self.num, data=self.data)


def parse_args():
    """
    Create an option parser, parse the result and return options and args.
    """
    parser = optparse.OptionParser()
    parser.add_option('-n', '--name', dest='name', default=None,
                      metavar='NAME', help='buildbot name')
    parser.add_option('-b', '--branch', dest='branch', default=None,
                      metavar='BRANCH', help='the Python branch (e.g. 2.6)')
    parser.add_option('-u', '--build', dest='build', default=None,
                      metavar='num', help='the build number of a buildslave')
    parser.add_option('-f', '--failures', dest='failures',
                      action='append', default=[],
                      metavar='test_xyz', help='the name of a failed test')

    options, args = parser.parse_args()
    #print options, args
    return options, args


def get_builder_list():
    # maybe there's a better way to get the list of buildbot...
    page = BeautifulSoup(urllib.urlopen(baseurl + 'all/one_box_per_builder'))
    builders = {}
    #builds = {}
    for row in page.find('table').findAll('tr'):
        name = str(row.find('td', {'class': 'box'}).a.string)
        if name not in builders:
            builders[name] = Builder(name)
        buildtd = row.find('td', {'class': re.compile('.*LastBuild.*')})
        if not buildtd.a:
            # no link to the last build
            continue
        lastbuild = buildtd.a['href'].split('/')[-1]
        builders[name].lastbuild = int(lastbuild)
    return builders


def main():
    options, args = parse_args()

    # find out the number of the last build from this page:
    builders = get_builder_list()

    # create the xmlrpc proxy to retrieve the build data
    proxy = xmlrpclib.ServerProxy(baseurl + 'all/xmlrpc')

    selected_builders = builders.values()

    # filter the builders according to the options
    if options.branch:
        selected_builders = [builder for name, builder in builders.iteritems()
                             if builder.branch == options.branch]
    if options.name:
        #names = options.name.split(',')
        pattern = fnmatch.translate(options.name)
        selected_builders = [builder for name, builder in builders.iteritems()
                             if re.match(pattern, name, re.I)]

    # loop through the builders and their builds
    for builder in selected_builders:
        print builder
        lastbuild = builder.lastbuild
        for buildnum in range(lastbuild, max(-1, lastbuild-5), -1):
            build = Build(buildnum)
            try:
                data = proxy.getBuild(str(builder), buildnum)
                build.data = data
            except xmlrpclib.Fault as f:
                if f.faultCode == 8002:
                    build.result = 'error (probably svn exception)'
            except xml.parsers.expat.ExpatError as e:
                build.result = 'error (can\'t parse server answer)'
            except Exception as e:
                build.result = 'error: %r' % e

            if (options.failures
                and not set(options.failures) <= set(build.failed_tests)):
                continue

            # this data should be accumulated in some object instead of be
            # printed directly, and there should be some 'reporter' object
            # able to print them out or generate other kind of reports
            # (e.g. HTML, XML, ...)
            print ' %5d:' % build.num,
            print build.result
            builder.builds[buildnum] = build

    return builders


if __name__ == '__main__':
    # set the builders var -- useful with python -i
    builders = main()
