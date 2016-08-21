# -*- coding: utf-8 -*-

import sys
# import datetime
import redis
# import logging
import yaml
import configparser
from terminaltables import AsciiTable


def usage():
    help = """
    help               --print this help
    list               --list all proxy groups
    list id            --list all redis instance of specified groups
    auth id password   --set password for specified proxy group
    auth all password  --set password for all proxy groups
    exit               --exit

***** Any other commands not in lists will be Ignoooored! *****
"""
    print help
    pass


def assembleURL(s):
    return s.split(":")[0] + ":" + s.split(":")[1]
    pass


def loadTwemproxyConfig():
    groups, instances, details = [], [], {}

    try:
        # 不要使用 configparser.ConfigParser() 。否则sql 里包含 % 会抛出解析错误
        # config = configparser.ConfigParser()
        config = configparser.RawConfigParser()
        config.read("ppm.conf")

        proxy = config.get("default", "proxy")
        timeout = config.getint("default", "timeout")

    except:
        print u"解析配置文件失败！"
        t, v, _ = sys.exc_info()
        print(t, v)
        sys.exit(1)
        pass

    y = yaml.load(file(proxy))

    groups = sorted(y.keys())

    # for k in groups:
    #     print k,y[k]["servers"]
    #     pass

    # for k in groups:
    #     print k
    #     pass

    groups_data = [
        ['id', 'proxy groups']
    ]
    for i, v in enumerate(groups):
        groups_data.append([i, v])
        pass
    table = AsciiTable(groups_data)
    print table.table

    # print u"build instances"
    for k in groups:
        instances.extend([assembleURL(url) for url in y[k]["servers"]])
        pass

    # print u"print instances"
    # for i,v in enumerate(instances):
    #     print i,v
    #     pass

    instances_data = [
        ['id', 'redis instances']
    ]
    for i, v in enumerate(instances):
        instances_data.append([i, v])
        pass
    table = AsciiTable(instances_data)
    print table.table

    # print u"build details"
    i = 0
    for k in groups:
        d = []
        for v in y[k]["servers"]:
            d.append({"id": i, "url": assembleURL(v)})
            i = i + 1
            pass
        details[k] = d
        pass

    # print u"print deatils"
    # for k in groups:
    #     print k,details[k]
    #     pass

    details_data = [
        ['id', 'redis instances']
    ]
    for i, v in enumerate(details[groups[0]]):
        details_data.append([v["id"], v["url"]])
        pass
    table = AsciiTable(details_data)
    print table.table

    return groups, instances, details
    pass


def checkAvailability(instances):
    # 检查所有 redis instances 是否可以连接的上, 如果不是都能连接上, 就不允许设置代理和 redis 实例的密码
    print instances
    return True
    pass


def is_ready(urls, oldpass):
    ready = True

    print "checking available of instances ..."
    for url in urls:
        if oldpass != "":
            r = redis.StrictRedis(host=url.split(":")[0],
                                  port=int(url.split(":")[1]),
                                  db=0,
                                  password=oldpass,
                                  socket_connect_timeout=1)
        else:
            r = redis.StrictRedis(host=url.split(":")[0],
                                  port=int(url.split(":")[1]),
                                  db=0,
                                  password=None,
                                  socket_connect_timeout=1)
            pass

        try:
            r.get(None)  # getting None returns None or throws an exception
            print "redis instance " + url + " is available."
        except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError):
            t, v, _ = sys.exc_info()
            # print v
            ready = False
            print "redis instance " + url + " is not available. " + str(v)
            # break
        except redis.exceptions.ResponseError:
            t, v, _ = sys.exc_info()
            # print v
            ready = False
            print "redis instance " + url + " is not available. " + "password is not correct,auth fail." if str(
                v) == "NOAUTH Authentication required." else str(v)
            # break

    return ready


def authRedis(url, oldpass, newpass):
    r = redis.StrictRedis(host=url.split(":")[0],
                          port=int(url.split(":")[1]),
                          db=0, password=oldpass,
                          socket_connect_timeout=1)
    r.config_set("requirepass", newpass)
    pass


def list(cmds, groups, instances, details):
    print cmds
    if len(cmds) == 1:
        groups_data = [
            ['id', 'proxy groups']
        ]
        for i, v in enumerate(groups):
            groups_data.append([i, v])
            pass
        table = AsciiTable(groups_data)
        print table.table
        pass
    else:
        try:
            id = int(cmds[1])
        except:
            id = -1
            pass
        if id >= 0 and id < len(groups):

            details_data = [
                ['id', 'redis instances\nof group ' + groups[id]]
            ]
            for i, v in enumerate(details[groups[id]]):
                details_data.append([v["id"], v["url"]])
                pass
            table = AsciiTable(details_data)
            print table.table

            pass
        else:
            print id
            pass
    pass


def auth(cmds, groups, instances, details, passwd):
    print cmds
    if len(cmds) < 3:
        print len(cmds)
        pass
    elif cmds[1] == "all":

        # if is_ready(instances, passwd[groups[id]]):
        #     print "...all redis instances are avariable!"
        #
        #     for url in instances:
        #         authRedis(url, passwd[groups[id]], cmds[2])
        #         pass
        #
        #     pass
        # else:
        #     print "...at least one of redis instances is not avariable!"
        #     pass

        for v in groups:
            if is_ready([x["url"] for x in details[v]], passwd[v]):
                print "...all redis instance are avariable!"

                for _, url in enumerate(details[groups[id]]):
                    authRedis(url["url"], passwd[v], cmds[2])
                    pass

                passwd[v] = cmds[2]
                syncPasswd(v, cmds[2])

            else:
                print "...at least one redis instances is not avariable!"
                pass


        pass
    # elif cmds[1] != "all":
    else:

        try:
            id = int(cmds[1])
        except:
            id = -1
            pass

        if id >= 0 and id < len(groups):
            # 正常范围内的 组 id

            # print [ x["url"] for x in details[groups[id]]]
            # ready = is_ready([ x["url"] for x in details[groups[id]]],"")
            if is_ready([x["url"] for x in details[groups[id]]], passwd[groups[id]]):
                print "...all redis instance are avariable!"

                for _, url in enumerate(details[groups[id]]):
                    authRedis(url["url"], passwd[groups[id]], cmds[2])
                    pass

                passwd[groups[id]] = cmds[2]
                syncPasswd(groups[id], cmds[2])

            else:
                print "...at least one redis instances is not avariable!"
                pass

            pass
        else:
            print id
            pass
    pass


def initPasswd(groups):
    passwd = {}
    try:
        # 不要使用 configparser.ConfigParser() 。否则sql 里包含 % 会抛出解析错误
        # config = configparser.ConfigParser()
        config = configparser.RawConfigParser()
        config.read("ppm.conf")

        # proxy = config.get("passwd", "proxy")
        # timeout = config.getint("default", "timeout")

        if config.has_section("passwd"):
            pass
        else:
            config.add_section("passwd")
            pass

        for v in groups:
            # print v
            if config.has_option("passwd", v):
                passwd[v] = config.get("passwd", v)
                pass
            else:
                passwd[v] = ""
                config.set("passwd", v, "")
                pass

        config.write(open("ppm.conf", "w"))

    except:
        print u"解析配置文件失败！"
        t, v, _ = sys.exc_info()
        print(t, v)
        # sys.exit(1)
        pass

    return passwd

    pass


def syncPasswd(optinon, newpass):
    try:
        # 不要使用 configparser.ConfigParser() 。否则sql 里包含 % 会抛出解析错误
        # config = configparser.ConfigParser()
        config = configparser.RawConfigParser()
        config.read("ppm.conf")

        if config.has_section("passwd"):
            pass
        else:
            config.add_section("passwd")
            pass

        config.set("passwd", optinon, newpass)

        config.write(open("ppm.conf", "w"))

    except:
        print u"解析配置文件失败！"
        t, v, _ = sys.exc_info()
        print(t, v)
        # sys.exit(1)
        pass

    pass


def passlist(passwd):
    passwd_data = [
        ['proxy groups', "passwd"]
    ]
    for k, v in passwd.items():
        passwd_data.append([k, v])
        pass
    table = AsciiTable(passwd_data)
    print table.table
    # print passwd


#####################################################################################

def main():
    usage()

    groups, instances, details = loadTwemproxyConfig()

    passwd = initPasswd(groups)

    while True:
        cmds = raw_input(">>").split()
        if len(cmds) == 0:
            continue
        elif cmds[0] == "help":
            usage()
        elif cmds[0] == "exit":
            print "Bye!\n"
            break
        elif cmds[0] == "list":
            list(cmds, groups, instances, details)
        elif cmds[0] == "auth":
            auth(cmds, groups, instances, details, passwd)
        elif cmds[0] == "pass":
            passlist(passwd)
        else:
            pass
        pass


#####################################################################################

if __name__ == "__main__":
    main()
    pass
