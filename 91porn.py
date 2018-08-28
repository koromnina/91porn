import requests
from bs4 import BeautifulSoup
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import random
import time
import sqlite3
import re
from lxml import etree

# 随机取httpheader
uas = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/58.0.3029.96 Chrome/58.0.3029.96 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0; Baiduspider-ads) Gecko/17.0 Firefox/17.0",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9b4) Gecko/2008030317 Firefox/3.0b4",
    "Mozilla/5.0 (Windows; U; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; BIDUBrowser 7.6)",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0",
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64; Trident/7.0; Touch; LCJB; rv:11.0) like Gecko",
]
baseUrl = 'http://93.91p12.space/v.php'
pages = 0


# 初始化sqlite3 数据库
def initDb():
    connection = sqlite3.connect("test91.db")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS url(id INTEGER PRIMARY KEY   AUTOINCREMENT,videoUrl TEXT UNIQUE,videoName TEXT,timetext TEXT,addtimetext TEXT,"
        "videoAuthorContent TEXT,videoAuthorUrl TEXT,viewNumber TEXT,likeNumber TEXT,commentNumber TEXT,flag INTEGER DEFAULT 0)")


# 随机生成http header
def setHeader():
    randomIP = str(random.randint(0, 255)) + '.' + str(random.randint(0, 255)) + '.' + str(
        random.randint(0, 255)) + '.' + str(random.randint(0, 255))
    headers = {
        'User-Agent': random.choice(uas),
        "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6",
        'X-Forwarded-For': randomIP,
    }
    return headers


def getContent(url, stream=False):
    try:
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=10, status_forcelist=[500, 502, 503, 504])
        s.mount('http://', HTTPAdapter(max_retries=retries))
        response = s.get(url, headers=setHeader(), stream=stream);
        response.encoding = response.apparent_encoding
        if (response.status_code == 200):
            return response
    except Exception as e:
        time.sleep(10)
        print("请求失败{0},reason={1}".format(url, e))
    return None


# 获取91总页数
def getPageNumber():
    html = getContent(baseUrl).content
    if (html):
        bs = BeautifulSoup(html, "html.parser")
        return bs.select(".page_number")[-1].find_all_previous("a")[1].text


# 抓取页面

def listUrl():
    pages = getPageNumber()
    page_count = int(pages)
    print("抓取到总共{0}个页面".format(page_count))
    if (int(page_count) > 0):
        currentPage = 1
        while (currentPage <= page_count):
            size = random.randint(10, 50)
            print("本次抓取{0}个页面".format(size))
            for page in range(currentPage, currentPage + size):
                FvUrl = baseUrl + '?category=mf&viewtype=basic&page=' + str(page)
                print("开始抓取第{0}页，地址为{1}".format(page, FvUrl))
                response = getContent(FvUrl).content
                if (response):
                    getUrlContent(response)
                else:
                    print("抓取页面失败")
            currentPage = size + currentPage
            downLoadBatch()
        downLoadBatch(-1)


def getUrlContent(html):
    soup = BeautifulSoup(html, "html.parser")
    videoContentList = soup.find('div', attrs={'id': 'videobox'})
    i = 0
    connection = sqlite3.connect("test91.db")
    for videoLi in videoContentList.find_all('div', attrs={'class': 'listchannel'}):
        videoName = videoLi.find('img', attrs={'width': '120'}).get('title').replace("'", "")
        videoUrl = videoLi.find('a', attrs={'target': 'blank'}).get('href')
        timetext = videoLi.select(".info")[0].next_sibling.strip()
        addtimetext = videoLi.select(".info")[1].next_sibling.strip()
        try:
            videoAuthorContent = videoLi.find('a', attrs={'target': '_parent'}).getText().replace("'", "")
        except AttributeError:
            videoAuthorContent = "None"
        try:
            videoAuthorUrl = videoLi.find('a', attrs={'target': '_parent'}).get('href')
        except AttributeError:
            videoAuthorUrl = "None"
        viewNumber = videoLi.select(".info")[3].next_sibling.strip()
        likeNumber = videoLi.select(".info")[4].next_sibling.strip()
        commentNumber = videoLi.select(".info")[5].next_sibling.strip()
        i += 1
        connection.execute("INSERT or replace INTO url(videoUrl ,videoName ,timetext ,addtimetext ,"
                           "videoAuthorContent ,videoAuthorUrl ,viewNumber ,likeNumber ,commentNumber) values( "
                           "'" + videoUrl + "','" + videoName + "','" + timetext + "','" + addtimetext + "','" + videoAuthorContent + ""
                                                                                                                                      "','" + videoAuthorUrl + "','" + viewNumber + "','" + likeNumber + "','" + commentNumber + "')")
    connection.commit()
    connection.close()


target_folder = ""


def downLoad(link):
    connection = sqlite3.connect("test91.db")
    sql = "UPDATE url set flag={0} WHERE videoUrl='{1}'"
    try:
        print("开始下载地址为{0}".format(link))
        content = getContent(link).content
        utext = content.decode('utf-8')
        soup2 = BeautifulSoup(utext, 'lxml')
        vurl = soup2.find('video').find('source').get('src')
        videoTitle = soup2.find(id='viewvideo-title').get_text().strip()
        fileType = re.findall('\.(.{3}?)\?', vurl)  # .mp4\.avi
        fileName = videoTitle + '.' + fileType[0]
        filePath = os.path.join(target_folder, fileName)
        if (not os.path.isfile(filePath)):
            res = getContent(vurl, stream=True)
            if (res):
                file = open(filePath, 'wb')
                for chunk in res.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
        else:
            print("文件已存在 本次不下载")
        sql = sql.format(1, link)
        print("下载完成")
    except Exception as e:
        print("下载失败{0}", e)
        # 更新为下载失败
        sql = sql.format(-1, link)
    connection.execute(sql)
    connection.commit()
    connection.close()


# 递归下载
def downLoadBatch(flag=0):
    connection = sqlite3.connect("test91.db")
    cursor = connection.execute("SELECT count(1) FROM url WHERE flag={0}".format(flag))
    urlList = []
    if (cursor):
        for i in cursor:
            totalCount = i[0]
        print("本次还有{0}条未下载".format(totalCount))
        if (totalCount > 0):
            cursor = connection.execute("SELECT videoUrl FROM url WHERE flag={0}".format(flag))
            for str in cursor:
                urlList.append(str[0])
            connection.close()
        if (len(urlList) > 0):
            for str in urlList:
                downLoad(str)
            downLoadBatch(flag)


if __name__ == '__main__':
    import os

    current_folder = os.getcwd()
    target_folder = os.path.join(current_folder, "91porn")
    if not os.path.isdir(target_folder):
        os.mkdir(target_folder)
    initDb()
    listUrl()
