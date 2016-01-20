#coding=utf-8
import multiprocessing
import happybase
from utl import *
import re
from lxml import etree
import gevent
import sys
# import time


ADDR = '192.168.109.220'
DB_NAME = 'p2p:ppdai'
CONNECTION = happybase.Connection(ADDR, autoconnect=False)
CONNECTION.open()
TABLE = CONNECTION.table(DB_NAME)
# CUR_TIME = time.strftime('%Y%m%d')
USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36'

SESSION1 = SessionInLock()
SESSION1.headers.update({
    'user-agent': USER_AGENT
})
SESSION2 = SessionInLock()
SESSION2.headers.update({
    'user-agent': USER_AGENT
})

BLACKL_PTN = re.compile(r'<input type="button" class="btn btn-primary" value="\xe6\x9f\xa5\xe7\x9c\x8b\xe8\xaf\xa6\xe6\x83\x85" onclick="javascript:window.location.href=\'(.*?)\'" />')
PAGES_PTN = re.compile(r'<td>\xe5\x85\xb1(\d+)\xe9\xa1\xb5</td>')
INFO_PTN = re.compile(r'<ul>.*<li>.*?<hr>(.*?)</p>.*?</li>')
MONRY_PTN = re.compile(r'.*?([\d,.]+).*?')


class BlackList:

    def __init__(self, url, page):
        self.url = url + str(page)


    def list_(self):
            url = self.url
            r = SESSION1.get(url)
            for j in BLACKL_PTN.findall(r.content):
                yield 'http://loan.ppdai.com' + j.strip()


class InfoDetail:
    def __init__(self, url):
        self.url = url
        self.re_count = 0

    def get_dict(self):
        r = SESSION2.get(self.url, sleep_time=0.1)
        page = etree.HTML(r.content.decode('utf-8'))
        try:
            info = INFO_PTN.findall(r.content.replace('\r\n', ''))[0].decode('utf-8')
        except IndexError as e:
            sleep(0.5)
            self.re_count += 1
            if self.re_count >= 4:
                return {'url': self.url, 'status': 'fail'}
            return self.get_dict()
        info_d = {}
        infs = {}
        for i in info.strip().split('<br>')[:-1]:
            k, v = i.strip().split(u'：')
            infs[k] = v.strip('*')
        tab = page.xpath(r'//*[@cellpadding="0"][2]/tr/td')
        i = 0
        t = []
        title = page.xpath('//*[@id="content_nav"]/div[2]/div[1]/div[1]/span')[0].text.split('_')
        while i < len(tab):
            t.append({
            u'列表编号': tab[i].text,
            u'借款期数': tab[i+1].text,
            u'借款时间': tab[i+2].text,
            u'逾期天数': tab[i+3].text,
            u'逾期本息': tab[i+4].text,
            })
            i += 5
        info_d.update({
            'info:loan_amt': MONRY_PTN.findall(page.xpath('//*[@class="detail_td"]')[0].text)[0].replace(',', ''), #u'累计借入本金'
            'info:max_overdue_days': page.xpath('//*[@class="detail_td"]/span')[0].text.strip(u'天').strip(), #u'最大逾期天数'
            'info:table': json.dumps(t), #u'表格'
            'info:region': title[1], #u'地区'
            'info:gender': title[2], #u'性别'
            'info:cert_id': title[3].strip('*') #u'身份证号'
        })
        info_d['info:phone'] = infs[u'手机号']
        info_d['info:name'] = infs[u'姓名']
        info_d['info:uname'] = infs[u'用户名']
        return info_d


def crawl_page(year, page):
    b = BlackList('http://loan.ppdai.com/blacklist/%s_m0_p'%year, page)
    with TABLE.batch(batch_size=1000) as bat:
            for i in b.list_():
                i = InfoDetail(i)
                d = i.get_dict()
                for i in d:
                    d[i] = d[i].encode('utf-8')
                if d.has_key('status'):
                    continue
                # q.put(d)
                bat.put('%s_%s_%s'%(d['info:cert_id'], d['info:phone'], d['info:name']), d)


def get_pages(url):
        r = SESSION1.get(url)
        try:
            return PAGES_PTN.findall(r.content)[0]
        except IndexError:
            return 1


def solve_year(year):
    print u'%d开始'%year
    pages = int(get_pages('http://loan.ppdai.com/blacklist/%d_m0'%year))
    for j in xrange(1,pages/20):
        gevent.joinall([gevent.spawn(crawl_page, year, i) for i in xrange(20*(j-1), 20*j)],)
    gevent.joinall([gevent.spawn(crawl_page, year, i) for i in xrange(pages-pages%20, pages+1)],)
    CONNECTION.close()
    print year, u'完成'


def get_years():
    r = SESSION1.get('http://loan.ppdai.com/blacklist/2015_m0')
    page = etree.HTML(r.content.decode('utf-8'))
    r = page.xpath('//*[@id="content_nav"]/div[2]/div[2]/div/ul/li/a')
    for i in r:
        try:
            j = int(i.text)
        except ValueError:
            continue
        yield j


def consume(q):
        print 0
    # with TABLE.batch(batch_size=1000) as bat:
        while not q.empty():
            d = q.get(block=True)
            sleep(0)
            # bat.put(d['phone:%s'%CUR_TIME]+d['cert_id:%s'%CUR_TIME]+d['name:%s'%CUR_TIME], d)
            print d

def test(a,q):
    print a,q


# q = multiprocessing.Queue(1001)
if __name__ == '__main__':
    # print table.row('1332755014042619901218申晨飞')
    # TABLE.put('123',{'info:name': '1232'})
    # print TABLE.row('123')
    # get_years()
    # q = multiprocessing.Queue(1001)
    pool = multiprocessing.Pool(processes=10)
    for year in get_years():
        pool.apply_async(solve_year, args=(year, ))
    # pool.apply_async(test, args=('1','2'))
    pool.close()
    pool.join()
