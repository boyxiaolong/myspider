# coding:utf-8
import requests
from threading import Timer
from threading import Lock
from utils import HtmlAnalyzer
from threading import Thread
from utils import HtmlAnalyzer
from Queue import Queue
import time


class Strategy(object):
    def __init__(self, max_depth, max_count, concurrency=5):
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
        self.strategy = Strategy(max_depth, max_count)
        self.queue = Queue()
        self.url_set = set()
        obj = UrlObj(root_url, 0)
        self.put(obj)
        self.handle_num = 0
        self.lock = Lock()
        self.thread_lock = Lock()
        self.threadpool = {}
        self.thread_id = 0
        self.is_stop = False
        self.thread_num = 0
        self.currency_limit = False
        self.last_data = None

    def put(self, obj):
        hash_val = hash(obj.url)
        if hash_val not in self.url_set:
            self.url_set.add(hash_val)
            self.queue.put(obj)

    def _run_loop(self):
        while True:
            if self.is_stop:
                time.sleep(1)
                continue

            if self.currency_limit:
                time.sleep(1)
                self.thread_lock.acquire()
                thread_num = len(self.threadpool)
                if thread_num == self.strategy.concurrency:
                    self.thread_lock.release()
                    continue
                else:
                    self.currency_limit = False
                self.thread_lock.release()
            else:
                try:
                    url = self.queue.get()
                except:
                    continue

            self.thread_id = self.thread_id + 1
            thd = Handler(url, self, self.thread_id)

            self.thread_lock.acquire()
            if len(self.threadpool) == self.strategy.concurrency:
                self.currency_limit = True
            self.threadpool[self.thread_id] = thd
            self.thread_lock.release()

            self.thread_num = self.thread_num + 1
            print 'add thread ', self.thread_id

            thd.start()
            self.handle_num = self.handle_num+1
            self.check_if_need_stop()

    def run(self):
        self.timer = Timer(self.strategy.time, self.stop)
        self.timer.start()
        self._run_loop()

    def stop(self):
        self.is_stop = True
        return

    def check_if_need_stop(self):
        if self.strategy.max_count <= self.handle_num:
            print 'handle_num %d is full' % self.handle_num
            self.stop()

    def is_dup_url(self, url):
        hash_val = hash(url)
        self.lock.acquire()
        res = hash_val in self.url_set
        self.lock.release()
        return res
    def remove_thread(self, thd_id):
        self.thread_lock.acquire()
        if thd_id in self.threadpool:
            del self.threadpool[thd_id]
            print 'del threadid ', thd_id
        self.thread_lock.release()

class Handler(Thread):
    def __init__(self, urlobj, spider, thd_id):
        Thread.__init__(self)
        print 'begin greenlet with url %s with thd %d' % (urlobj.url, thd_id)
        self.urlobj = urlobj
        self.spider = spider
        self.thread_id = thd_id
        self.charset = "utf-8"

    def run(self):
        try:
            html = self.open(self.urlobj.url)
        except Exception, why:
            return

        depth = self.urlobj.depth + 1

        if depth > self.spider.strategy.max_depth:
            return

        for link in self.feed(html):
            if self.spider.is_dup_url(link):
                continue
            url = UrlObj(link, depth)
            self.spider.put(url)
        self.spider.remove_thread(self.thread_id)

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

test = MySpider(max_depth=20, max_count=50, root_url="http://www.maiziedu.com")
test.run()