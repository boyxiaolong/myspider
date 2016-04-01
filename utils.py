#!/usr/bin/env python
#-*- coding: utf-8 -*-

import re
import urllib
import urlparse
from pyquery import PyQuery
import domain
import os.path

class HtmlAnalyzer(object):

    @staticmethod
    def detectCharSet(html):

        pq = PyQuery(html)

        metas = pq('head')('meta')

        for meta in metas:
            for key in meta.keys():
                #print "key ", key
                if key == "charset":
                    charset = meta.get('charset')
                    return charset
                elif key == "content":
                    try:
                        content = meta.get('content')
                        p = re.match(r".+charset=(.*)\W*", content)
                        charset = p.group(1)
                        return charset
                    except:
                        continue

    @staticmethod
    def extractLinks(html, baseurl, charset):

        def _extract(url, attr):
            link = url.attrib[attr]
            link = link.strip("/ ").strip('\\"')
            if link is None:
                raise

            link = urlparse.urljoin(baseurl, link)
            link = urlparse.urldefrag(link)[0]

            try:
                link = urllib.quote(link, ':?=+&#/@')
            except (UnicodeDecodeError, KeyError):
                try:
                    link = urllib.quote(link.encode(charset), ':?=+&#/@')
                except:
                    pass

            return link

        def _isValidLink(url):
            try:
                return all([UrlFilter.checkScheme(url),
                            UrlFilter.checkInvalidChar(url),
                            UrlFilter.checkInvalidExtention(url)
                            ])
            except:
                return False

        pq = PyQuery(html)

        allLinks = []

        for url in pq('a'):
            try:
                link = _extract(url, 'href')
            except:
                continue
            if _isValidLink(link):
                allLinks.append(link)

        for url in pq('form'):
            try:
                link = _extract(url, 'action')
            except:
                continue
            if _isValidLink(link):
                allLinks.append(link)
        return allLinks


class UniqRule(object):

    # 用于形如abc123格式
    alnum = re.compile(r'^(\D+)(\d+)$')

    date = re.compile(r'^([12]\d)?\d\d-\d{1,2}(-\d{1,2})?$')

    connector = '|'

    # 相同后缀名
    ext = {
        '.asp':  '.asp',
        '.aspx': '.asp',
        '.jsp':  '.jsp',
        '.jspx': '.jsp',
    }

    scheme = {
        'http':  'http',
        'https': 'http'
    }

    normalize_dict = {
        'digit':  '1',
        'letter': 'a',
        'date':   '2013-01-01',
    }

    def __init__(self, depth=None):
        self.depth = depth

    def is_digit(self, param):
        return param.isdigit()

    def is_letter(self, param):
        return len(param) == 1 and param.isalpha()

    def is_alnum(self, param):
        if UniqRule.alnum.match(param):
            return True
        return False

    # 形如abc-123-453
    def is_hyphen_split(self, param):
        return not param.find('-') == -1

    def is_underscore_split(self, param):
        return not param.find('_') == -1

    def is_date(self, param):
        if UniqRule.date.match(param):
            return True
        return False

    def split_params(self, pathnode):
        name_params = pathnode.split(';')
        if len(name_params) > 1:
            return name_params[0], name_params[1:]
        else:
            return name_params[0], []

    def normalize(self, param):
        if self.is_digit(param):
            return UniqRule.normalize_dict['digit']
        elif self.is_letter(param):
            return UniqRule.normalize_dict['letter']
        elif self.is_date(param):
            return UniqRule.normalize_dict['date']
        elif self.is_alnum(param):
            match = UniqRule.alnum.match(param)
            return match.group(1) + UniqRule.normalize_dict['digit']
        elif self.is_hyphen_split(param):
            params = param.split('-')
            for k, v in enumerate(params):
                if v.isdigit():
                    params[k] = UniqRule.normalize_dict['digit']
            return '-'.join(params)
        elif self.is_underscore_split(param):
            params = param.split('_')
            for k, v in enumerate(params):
                if v.isdigit():
                    params[k] = UniqRule.normalize_dict['digit']
            return '_'.join(params)
        else:
            return param

    ############################################################

    def is_depth_set(self):
        return self.depth is not None

    def normalize_scheme(self, scheme):
        return UniqRule.scheme.get(scheme, scheme)

    def normalize_hostname(self, hostname):
        return hostname

    def normalize_dirs(self, dir_list):
        dir_depth = len(dir_list)
        if self.is_depth_set() and self.depth <= dir_depth:
            return UniqRule.connector.join([self.normalize(dir_list[i])
                                            for i in xrange(self.depth)])
        return UniqRule.connector.join([self.normalize(dir_list[i])
                                        for i in xrange(dir_depth)])

    def normalize_tailpage(self, tailpage):
        try:
            tpname, params = self.split_params(tailpage)
        except IndexError:
            return tailpage
        fname, ext = os.path.splitext(tpname)
        norm_name = self.normalize(fname)
        norm_ext = UniqRule.ext.get(ext, ext)
        norm_params = sorted(params)
        result = [norm_name, norm_ext]
        result.extend(norm_params)
        return UniqRule.connector.join(result)

    def normalize_querykeys(self, querykeys):
        return UniqRule.connector.join(sorted(querykeys))


class UrlFilter(object):

    invalid_chars = {'\'': None,
                     '\"': None,
                     '\\': None,
                     ' ': None,
                     '\n': None,
                     '\r': None,
                     '+': None
                     }

    invalid_extention = {
        'jpg':  None,
        'gif':  None,
        'bmp':  None,
        'jpeg':  None,
        'png':  None,

        'swf':  None,
        'mp3':  None,
        'wma':  None,
        'wmv':  None,
        'wav':  None,
        'mid':  None,
        'ape':  None,
        'mpg':  None,
        'mpeg':  None,
        'rm':  None,
        'rmvb':  None,
        'avi':  None,
        'mkv':  None,

        'zip':  None,
        'rar':  None,
        'gz':  None,
        'iso':  None,
        'jar':  None,

        'doc':  None,
        'docx':  None,
        'ppt':  None,
        'pptx':  None,
        'chm':  None,
        'pdf':  None,

        'exe':  None,
        'msi':  None,
    }

    @staticmethod
    def checkScheme(url):
        scheme, netloc, path, pm, q, f = urlparse.urlparse(url)
        return scheme in ('http', 'https')

    @classmethod
    def checkInvalidChar(cls, url):
        exist_invalid_char = False
        for c in url:
            if c in cls.invalid_chars:
                exist_invalid_char = True
                break
        return (not exist_invalid_char)

    @classmethod
    def checkInvalidExtention(cls, url):
        dotpos = url.rfind('.') + 1
        typestr = url[dotpos:].lower()
        return (typestr not in cls.invalid_extention)

    @staticmethod
    def isSameDomain(first_url, second_url):
        fhost = urlparse.urlparse(first_url).netloc
        shost = urlparse.urlparse(second_url).netloc
        return (domain.GetFirstLevelDomain(fhost) ==
                domain.GetFirstLevelDomain(shost))

    @staticmethod
    def isSameHost(first_url, second_url):
        return urlparse.urlparse(first_url).netloc == urlparse.urlparse(second_url).netloc

    @staticmethod
    def isSameSuffixWithoutWWW(first_url, second_url):
        fhost = '.' + urlparse.urlparse(first_url).netloc
        shost = '.' + urlparse.urlparse(second_url).netloc

        if shost[:5] == '.www.':
            shost = shost[5:]

        if fhost.find(shost) != -1:
            return True
        else:
            return False

    # check whether first_url has the suffix second_url
    @staticmethod
    def isSameSuffix(first_url, second_url):
        fhost = '.' + urlparse.urlparse(first_url).netloc
        shost = '.' + urlparse.urlparse(second_url).netloc

        if fhost.find(shost) != -1:
            return True
        else:
            return False


    # remove similary urls
    @staticmethod
    def uniq(urls, rule=UniqRule()):
        result = {}
        for u in urls:
            try:
                urlobj = UrlObject(u, rule)
            except Exception:
                result[hash(u)] = u
                continue
            result.setdefault(urlobj.hashcode, u)
        return result.values()
