#coding=utf-8
from utl import *
from lxml import etree
import re
import json
import happybase

SESSION = SessionInLock()
USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36'
SESSION.headers.update({
    'user-agent': USER_AGENT
})

ADDR = '192.168.109.220'
DB_NAME = 'p2p:anxindai'
CONNECTION = happybase.Connection(ADDR, autoconnect=False)
CONNECTION.open()
TABLE = CONNECTION.table(DB_NAME)

class AnxinDai:
    def __init__(self, url):
        self.url = url

    def get_dict(self):
        pages = 3
        for p in xrange(pages):
            r = SESSION.get(self.url%(p+1))
            page = etree.HTML(r.content.decode('utf-8'))
            for i in page.xpath('//*[@class="itemuser"]'):
                ret = {}
                baseinfo = i.xpath('.//div[@class="baseinfo"]/ul/li')
                conninfo = i.xpath('.//div[@class="conninfo"]/ul/li')
                overinfo = i.xpath('.//div[@class="overinfo"]/ul/li')
                for j in baseinfo+conninfo+overinfo:
                    try:
                        k, v = j.text.split(u'：')
                    except AttributeError:
                        continue
                    ret[k.strip()] = v.strip()
                detail_id = i.xpath('.//div[@class="showbtn"]/input')[0].attrib['onclick'][-7:-1]
                ret['table'] = SESSION.get('http://www.anxin.com/ajax/ajax_borrow.ashx?cmd=getpenaltyborrowlist&uid=%s'%detail_id).content.decode('utf-8')
                yield ret

if __name__ == '__main__':
    ad = AnxinDai('http://www.anxin.com/invest/overduelist_p%d.html')
    with TABLE.batch(batch_size=1000) as bat:
        for i in ad.get_dict():
            d = {}
            for j in i:
                d['info:'+j.encode('utf-8')] = i[j].encode('utf-8')
            # print json.dumps(d)
            bat.put('%s_%s_%s'%(d['info:身份证'],d['info:手机'],d['info:姓　名']) ,d)
