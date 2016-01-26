#coding=utf-8
import requests
import threading
from gevent import sleep
import json


class SessionInLock(requests.Session):
    """
    Session类，使用时调用get_url方法，等于session.get，里面封装了锁和sleep，控制网络访问
    """
    def __init__(self):
        requests.Session.__init__(self)
        self.lock = threading.RLock()
    def get(self, url, headers=None, sleep_time=0):

        self.lock.acquire()
        sleep(sleep_time)
        response_ = requests.Session.get(self, url, headers=headers, verify=False)
        self.lock.release()
        return response_

    def post(self, url, data=None, json=None, **kwargs):
        self.lock.acquire()
        sleep(kwargs['sleep_time'])
        response_ = requests.Session.get(self, url, headers=kwargs['headers'], verify=False)
        self.lock.release()
        return response_

def formatCookie(s):
    """把符合标准的字符串s转换成dict返回
    :type s: str
    :rtype :dict
    """
    jd = {}
    for each in s.replace(' ', '').split(';'):
        [k,v] = each.split('=',1);
        jd[k] = v.decode('gbk');
    return jd

if __name__ == '__main__':
    import sys, os
    cookies = input('输入cookies：')
    CWD = os.path.split( os.path.realpath(sys.argv[0]))[0]
    with open(CWD+'\\c2.json', 'w') as f:
        f.write(json.dumps(formatCookie(cookies)))
    print 'cookies已更新'