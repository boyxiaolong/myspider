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
    def __init__(self,max_depth, max_count, concurrency=5):
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
        self.url_set = set()
        obj = UrlObj(root_url, 0)
        self.put(obj)
        self.handle_num = 0

    def put(self, obj):
        hash_val = hash(obj.url)
        if hash_val not in self.url_set:
            self.url_set.add(hash_val)
            self.queue.put(obj)

    def _run_loop(self):
        while self.timer.isAlive():
            for greenlet in list(self.pool):
                if greenlet.dead:
                    self.pool.discard(greenlet)
            try:
                url = self.queue.get()
            except queue.Empty:
                continue

            greenlet = Handler(url, self)
            self.pool.start(greenlet)
            self.handle_num = self.handle_num+1
            self.check_if_need_stop()

    def run(self):
        self.timer = Timer(self.strategy.time, self.stop)
        self.timer.start()
        self._run_loop()

    def stop(self):
        self.timer.cancel()
        self.pool.join()
        return

    def check_if_need_stop(self):
        if self.strategy.max_count <= self.handle_num:
            print 'handle_num %d is full' %self.handle_num
            self.stop()

class Handler(gevent.Greenlet):
    def __init__(self, urlobj, spider):
        print 'begin greenlet with url', urlobj.url
        gevent.Greenlet.__init__(self)
        self.urlobj = urlobj
        self.spider = spider
        self.charset = "utf-8"

    def _run(self):
        try:
            html = self.open(self.urlobj.url)
        except Exception, why:
            return

        depth = self.urlobj.depth + 1

        if depth > self.spider.strategy.max_depth:
            return

        for link in self.feed(html):
            if hash(link) in self.spider.url_set:
                continue
            url = UrlObj(link, depth)
            self.spider.put(url)

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


class MySpider(object):
    def __init__(self, max_depth, max_count, root_url):
        self.spider = GeventSpider(max_depth=max_depth, max_count=max_count, root_url=root_url)

    def run(self):
        self.spider.run()

test = MySpider(max_depth=20, max_count=100, root_url="http://www.maiziedu.com")
test.run()