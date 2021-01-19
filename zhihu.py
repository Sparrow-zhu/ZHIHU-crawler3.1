import requests
import get_data
import time
import datetime
import locale
import re
import json
import izhiqunDB
import pandas as pd


id = 0
date = ''
title = ''
ans_link = ''
looked = 0
followers = 0
totals = 1 # 获取当下话题的总回答数
rank = 1 # 小鹿回答的排名，初始化为第1名，每次爬取完一个时需要再次初始化

final_data = [] # 最终数据
comment = [] # 用来存放数据
comments = [] # 用来存放数据
flag = 0 # 爬取小鹿排名时，需要flag来判断是否抓取到了小鹿的回答

today = datetime.datetime.today().strftime('%Y_%m_%d')

def save_data(comments):
    '''
    功能：将comments中的信息输出到文件中/或数据库中。
    参数：comments 将要保存的数据
    '''
    filename = f'/Users/psyduck/PycharmProjects/ZHIHU-crawler-小鹿2/小鹿{today}.csv'

    dataframe = pd.DataFrame(comments)
    dataframe.to_csv(filename, mode = 'a', index = False, sep = ',', header = False)

# 获取话题下回答者的用户信息，顺便获得排名，可以和parse_ansajax()函数合在一起，但是太混乱了所以拆开并用了全局变量来处理
def get_user_data(html_ans_json):

    global id
    global rank
    global flag
    global comment
    global comments
    flag = 0
    comments = []

    json_data = json.loads(html_ans_json)['data']
    #print(json_data)

    for item in json_data:
        comment = []
        comment_xiaolu = []  # 判断是否循环到了探长，如果判定是探长，则停止循环并且返回rank
        comment_xiaolu.append(item['author']['url_token'])  # 姓名，找中文名字容易出现乱码，所以使用url的信息
        comment_xiaolu.append(item['author']['name'])
        # comment.append(item['author']['gender'])  # 性别
        # comment.append(item['author']['url'])     # 个人主页
        # comment.append(item['voteup_count'])  # 点赞数
        # comment.append(item['comment_count'])  # 评论数
        # comment.append(item['url'])               # 回答链接
        # comments.append(comment)
        print(comment_xiaolu[0], comment_xiaolu[1])
        if comment_xiaolu[0] == "you-wu-jun-77" or comment_xiaolu[1] == "小鹿" or rank > 99:  # 如果小鹿回答的排名掉出100名以外变直接返回rank=100
            flag = 1
            id = id + 1

            comment.append(id)
            comment.append(date)
            comment.append(title)
            comment.append(ans_link)
            comment.append(looked)
            comment.append(followers)
            comment.append(totals)
            comment.append(rank)
            comments.append(comment)

            save_data(comments)

            print(comments)
            break
        else:
            rank += 1

    return comments


# 获得从【探长的回答界面】进入的探长的话题回答中的json数据，主要是获得next的json
def parse_ansajax(ans_json):

    global totals
    global rank
    global final_data
    global comment
    global comments
    global id

    print("=========哎呀，发现一条回答=========")

    html_ans_json = get_data.get_data(ans_json)

    #######################用来判断处理网页不返回请求信息#######################
    if isinstance(html_ans_json, str) != True:
        print("giao 这一条拒绝了我的访问1 giao")

        time.sleep(5) #网页不返回请求信息，就再请求一次，给出5秒的缓冲时间，请求太频繁了容易出现这样问题
        parse_ansajax(ans_json)
        return
    ######################################################################

    totals = json.loads(html_ans_json)['paging']['totals']
    print(f"当下回答总数数量：{totals}")

    rank = 1  # 重新初始化rank
    ans_page = 0

    while (ans_page <= totals):  # 遍历某话题下的每一条回答(用来获取回答者信息)(第二层循环)

        print("现在是多少页了：" + str(ans_page))
        html_ans_json_next = get_data.get_data(ans_json)

        #######################用来判断处理网页不返回请求信息#######################
        if isinstance(html_ans_json_next, str) != True:

            print("giao 这一条拒绝了我的访问2 giao")

            time.sleep(5) #网页不返回请求信息，就再请求一次，给出5秒的缓冲时间，请求太频繁了容易出现这样问题
            parse_ansajax(ans_json)
            return
        ######################################################################

        commentsss = get_user_data(html_ans_json_next)  # 获取探长的用户的信息，以及回答的rank

        ans_page += 5          #话题下面每个问题打开自动会加载5条回答

        if (flag != 0): # 如果已经抓取到了探长的排名，跳出函数
            break

        ans_json = json.loads(html_ans_json_next)['paging']['next']  # 获取某一个话题中的下一页回答（ps：每一页有5个回答，即offset=0，5，10...）

    ################################################
    # 没有抓取到探长，意味着本条信息有问题，比如回答侵权被下架，或者被拒绝了请求，那么就跳出循环开始下一个回答的抓取
    if (flag == 0):
        id = id + 1
        # print(rank)
        comment = []
        commentsss = []
        comment.append(id)
        comment.append(date)
        comment.append(title)
        comment.append(ans_link)
        comment.append(looked)
        comment.append(followers)
        comment.append(totals)
        comment.append(-1)
        commentsss.append(comment)

        save_data(commentsss)

        print(commentsss)
    ################################################

    final_data.extend(commentsss)  # 将所有获得的数据输入一个数组中

    return


# 获得从【探长的回答界面】进入的探长的话题回答中的直观数据：编辑日期、关注人数、浏览量
def parse_anslink(ans_link):

    global title
    global date
    global looked
    global followers
    global id
    global comment
    global comments
    global final_data

    # 转化 str型且含有逗号 的数字为int型
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

    ########### 请求过于频繁可能会遇到【('Connection aborted.', ConnectionResetError(54, 'Connection reset by peer'))】这个问题，忽略掉 ###########
    try:
        # 获取话题的关注人数,浏览量和编辑日期
        html_ans_link = get_data.get_data(ans_link)

        #######################用来判断处理网页不返回请求信息#######################
        if isinstance(html_ans_link, str) != True:

            print("giao 这一条拒绝了我的访问3 giao")

            time.sleep(5) #网页不返回请求信息，就再请求一次，给出5秒的缓冲时间，请求太频繁了容易出现这样问题
            parse_anslink(ans_link)
            return
        ######################################################################

        title = re.findall('<h1 class="QuestionHeader-title">(.*?)</h1>', html_ans_link)[0]
        date = re.findall('<span data-tooltip="(.*?)</span>', html_ans_link)[0].replace('">', ' > ')
        followers = locale.atoi(re.findall('关注者</div><strong class="NumberBoard-itemValue" title="(.*?)"', html_ans_link)[0])
        looked = locale.atoi(re.findall('被浏览</div><strong class="NumberBoard-itemValue" title="(.*?)"', html_ans_link)[0])

    except requests.exceptions.ConnectionResetError:
        print(ans_link)
        print('Handle Exception1')

    except requests.exceptions.ConnectionError:
        print(ans_link)
        print('Handle Exception2')

    return



def get_user_ans(link):
    global ans_link
    global id
    global comment
    global comments
    global final_data
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    print("～～～～～～～～～～～～～～～～～feed～～～～～～～～～～～～～～～～～")
    print(link)

    html_data = get_data.get_data(link)  # 返回的就是response.text

    #######################用来判断处理网页不返回请求信息#######################

    ####### giao4这块request如果出问题了需要重新爬取feed流链接不？有待商榷，等到出现giao4的问题再处理 #######

    if isinstance(html_data, str) != True:
        print("giao 这一条拒绝了我的访问4 giao")

        time.sleep(5) #网页不返回请求信息，就再请求一次，给出5秒的缓冲时间，请求太频繁了容易出现这样问题
        get_user_ans(link)
    ######################################################################

    infor_list = re.findall('\{"target":(.*?)"feed", "id":(.*?)\}', html_data)

    for data in infor_list:

        # 此时data是元组，需要转换成str类型用来正则
        data = str(data)
        if_answer = re.findall('"verb": "(.*?)"',data)[0]

        # 只有'ANSWER_CREATE'的情况是回答问题，需要爬取
        if if_answer == 'ANSWER_CREATE':

            time.sleep(0.3)

            link_id1 = re.findall('"https://api.zhihu.com/questions/(.*?)"', data)[0]
            link_id2 = re.findall('"url": "https://api.zhihu.com/answers/(.*?)"', data)[0]

            # 获得探长的回答链接
            ans_link = 'https://www.zhihu.com/question/' + link_id1 + '/answer/' + link_id2

            # 获得探长回答的话题的json
            # 知乎页面有时候会改后面的limit=5&offset=0&platform=desktop&sort_by=default，有时候会将limit改成3，一直用5就好，limit=5的json中的[pagging][next]中也是limit=5
            ans_json = 'https://www.zhihu.com/api/v4/questions/' + link_id1 + \
                       '/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics%3Bsettings.table_of_content.enabled%3B&limit=5&offset=0&platform=desktop&sort_by=default'

            # 分析探长的回答链接
            parse_anslink(ans_link)

            # 分析探长的ajax的回答链接
            parse_ansajax(ans_json)

    judge = json.loads(html_data)
    # 判断feed中还有没有下一页，返回的是bool值
    feed_is_end = judge['paging']['is_end']
    print(feed_is_end)

    # 判断更新日期，如果是2019年之前的就不要了，所有feed中answer的信息块的updated_time都是按由大到小的顺序排列的
    updated_time0 = locale.atoi(re.findall('"updated_time": (.*?),', html_data)[0])
    updated_time1 = locale.atoi(re.findall('"updated_time": (.*?),', html_data)[1])

    if feed_is_end == False and (updated_time0 > 1546300800 or updated_time1 > 1546300800):

        # 进入下一个feed流信息
        next_link = re.findall('"next": "(.*?)"', html_data)[0]
        get_user_ans(next_link)

        time.sleep(0.5)

    else:

        izhiqunDB.create_table()
        for info in final_data:
            izhiqunDB.insert(info)
        #izhiqunDB.regulate()
        print("所有数据爬取完毕！")

    return