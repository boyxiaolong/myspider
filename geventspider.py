# coding:utf-8
import gevent
from gevent import (monkey,
                    queue,
                    event,
                    pool)

import requests
from threading import Timer
from utils import HtmlAnalyzer

class Strategy(object):
    def __init__(self,max_depth, max_count, concurrency=10):
        self.max_depth = max_depth
        self.max_count = max_count
        self.concurrency = concurrency
        self.timeout = 60
        self.time = 12*3600

class UrlObj(object):
    def __init__(self, url, depth):
        self.url = url.strip('/')
        self.depth = depth

class GeventSpider(object):
    def __init__(self, max_depth, max_count, root_url):
        monkey.patch_all()
        self.strategy = Strategy(max_depth, max_count)
        self.queue = queue.Queue()
        self.pool = pool.Pool(self.strategy.concurrency)
        self.greenlet_finished = event.Event()
        self._stop = event.Event()
        self.url_set = set()
        obj = UrlObj(root_url, 0)
        self.put(obj)
        self.url_num = 0

    def put(self, obj):
        if obj.url not in self.url_set:
            self.url_set.add(obj.url)
            self.queue.put(obj)

    def run(self):
        self.timer = Timer(self.strategy.time, self.stop)
        self.timer.start()

        while not self.stopped() and self.timer.isAlive():
            if self.url_num >= self.strategy.max_count:
                print 'need stop'
                self.stop()
                return
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
            self.url_num = self.url_num+1

    def stopped(self):
        return self._stop.is_set()

    def stop(self):
        self.timer.cancel()
        self._stop.set()
        self.pool.join()
        self.queue.put(StopIteration)
        return


class Handler(gevent.Greenlet):
    def __init__(self, urlobj, spider):
        print 'begin greenlet with url', urlobj.url
        gevent.Greenlet.__init__(self)
        self.urlobj = urlobj
        self.spider = spider
        self.charset = "utf-8"

    def _run(self):
        strategy = self.spider.strategy
        urltable = self.spider.url_set

        try:
            html = self.open(self.urlobj.url)
        except Exception, why:
            return self.stop()

        depth = self.urlobj.depth + 1

        if strategy.max_depth and (depth > strategy.max_depth):
            return self.stop()

        for link in self.feed(html):
            if link in urltable:
                continue
            url = UrlObj(link, depth)
            self.spider.put(url)

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

test = MySpider(max_depth=20, max_count=30, root_url="http://www.douban.com")
test.run()