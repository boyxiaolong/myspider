# coding:utf-8
import gevent
from gevent import (monkey,
                    queue,
                    event,
                    pool)

import re
import sys
import logging
import urllib
import urlparse
import requests
from threading import Timer
from pyquery import PyQuery
from utils import HtmlAnalyzer, UrlFilter


class Strategy(object):
    def __init__(self,max_depth=5, max_count=5000, concurrency=5):
        self.max_depth = max_depth
        self.max_count = max_count
        self.concurrency = concurrency
        self.timeout = 10
        self.time = 6*3600

class UrlObj(object):
    def __init__(self, url, depth=0, linkin=None):
        self.url = url.strip('/')
        self.depth = depth
        self.linkin = linkin

    def __hash__(self):
        return hash(self.url)


class UrlTable(object):
    infinite = float("inf")

    def __init__(self, size=0):
        self.__urls = {}

        if size == 0 :
            size = self.infinite
        self.size = size

    def __len__(self):
        return len(self.__urls)

    def __contains__(self, url):
        return hash(url) in self.__urls.keys()

    def __iter__(self):
        for url in self.urls:
            yield url

    def insert(self, url):
        if isinstance(url, basestring):
            url = UrlObj(url)
        if url not in self:
            self.__urls.setdefault(hash(url), url)

    @property
    def urls(self):
        return self.__urls.values()

    def full(self):
        return len(self) >= self.size


class GeventSpider(object):
    def __init__(self, max_depth, max_count, root_url):
        monkey.patch_all()
        print max_depth, max_count
        self.strategy = Strategy(max_depth, max_count)
        self.queue = queue.Queue()
        self.urltable = UrlTable(self.strategy.max_count)
        self.pool = pool.Pool(self.strategy.concurrency)
        self.greenlet_finished = event.Event()
        self._stop = event.Event()
        self.setRootUrl(root_url)

    def setRootUrl(self,url):
        if isinstance(url, basestring):
            url = UrlObj(url)
        self.root = url
        self.put(self.root)

    def put(self, url):
        if url not in self.urltable:
            self.queue.put(url)

    def run(self):
        self.timer = Timer(self.strategy.time, self.stop)
        self.timer.start()
        print ("spider '%s' begin running" % self.root)

        while not self.stopped() and self.timer.isAlive():
            for greenlet in list(self.pool):
                if greenlet.dead:
                    self.pool.discard(greenlet)
            try:
                url = self.queue.get_nowait()
            except queue.Empty:
                if self.pool.free_count() != self.pool.size:
                    self.greenlet_finished.wait()
                    self.greenlet_finished.clear()
                    continue
                else:
                    self.stop()
            greenlet = Handler(url, self)
            self.pool.start(greenlet)

    def stopped(self):
        return self._stop.is_set()

    def stop(self):
        self.timer.cancel()
        self._stop.set()
        self.pool.join()
        self.queue.put(StopIteration)
        return

    def dump(self):
        import StringIO
        out = StringIO.StringIO()
        for url in self.urltable:
            try:
                print >> out ,url
            except:
                continue
        return out.getvalue()


class Handler(gevent.Greenlet):
    def __init__(self, urlobj, spider):
        print 'begin greenlet with url', urlobj.url
        gevent.Greenlet.__init__(self)
        self.urlobj = urlobj
        self.spider = spider
        self.charset = "utf-8"

    def _run(self):
        strategy = self.spider.strategy
        urltable = self.spider.urltable
        queue = self.spider.queue

        try:
            html = self.open(self.urlobj.url)
        except Exception, why:
            return self.stop()

        linkin = self.urlobj
        depth = linkin.depth + 1

        if strategy.max_depth and (depth > strategy.max_depth):
            return self.stop()

        for link in self.feed(html):
            if urltable.full():
                self.stop()
                self.spider.stop()
                return

            if link in urltable:
                continue

            url = UrlObj(link, depth, linkin)
            urltable.insert(url)
            queue.put(url)

        self.stop()

    def open(self, url):
        strategy = self.spider.strategy
        try:
            resp = requests.get(url, timeout=strategy.timeout)
        except requests.exceptions.RequestException, e:
            raise e
        if resp.status_code != requests.codes.ok:
            resp.raise_for_status()
        charset = HtmlAnalyzer.detectCharSet(resp.text)
        if charset is not None:
            self.charset = charset
            resp.encoding = charset
        return resp.text

    def feed(self,html):
        return HtmlAnalyzer.extractLinks(html,self.urlobj.url,self.charset)


    def stop(self):
        self.spider.greenlet_finished.set()
        self.kill(block=False)


class MySpider(object):
    def __init__(self, max_depth, max_count, root_url):
        self.spider = GeventSpider(max_depth=max_depth, max_count=max_count, root_url=root_url)

    def run(self):
        self.spider.run()

test = MySpider(max_depth=10, max_count=5000, root_url="http://www.douban.com")
test.run()