#pip lib
import sys
import os
import time
import datetime
import logging
import configparser
from pathlib import Path
import multiprocessing as mp

#local module
from darknet import Darknet
from thirdc import getpoints
from dbcc import Dbcc

class Server:
    def __init__(self, darknet, logger, dbcc):
        self.detector = darknet
        self.logger = logger
        self.db = dbcc
        self.jobs = mp.Queue()
    
    def porter(self):
        while True:
            rr = self.db.get_job()
            self.logger.info("Query the undetect record from DB num:" + str(len(rr)))
            if(len(rr) is 0):
                time.sleep(10)
            else:
                for r in rr:
                    self.logger.debug("r in rr-->" + str(r))
                    job = {'fid':r[0],'fpath':r[2]}
                    if(Path(r[2]).suffix is 'mp4'):
                        job['type'] = 0
                    else:
                        job['type'] = 1
                    self.logger.debug("job-->" + str(job))
                    self.jobs.put(job)
                    # self.db.updatefilestatus(2, job['fid'])
    
    def img_worker(self):
        pass

    def mp4_worker(self):
        pass

    def run(self):
        mpPorter = mp.Process(target=self.porter)
        mpPorter.start()

def initlog():
    logger = logging.getLogger()
    logger.setLevel('DEBUG')
    log_format="[%(asctime)s]\t[%(levelname)s]\t%(message)s"
    date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, date_format)
    verb = logging.StreamHandler()
    verb.setFormatter(formatter)
    verb.setLevel('DEBUG')
    logger.addHandler(verb)
    fhlr = logging.FileHandler(datetime.datetime.now().strftime("log/tk%Y-%m-%d_%H_%M_%S.log"))
    fhlr.setFormatter(formatter)
    logger.addHandler(fhlr)
    return logger

def get_params(configfilepath):
    """
    getting parameter from config
    """
    config = configparser.ConfigParser()
    config.read(configfilepath)

    try:
        # for Yolo
        darknetlibfilepath = config.get('Yolo', 'darknetlibfilepath')
        datafilepath = config.get('Yolo', 'datafilepath')
        cfgfilepath = config.get('Yolo', 'cfgfilepath')
        weightfilepath = config.get('Yolo', 'weightfilepath')

        yoloc = {'darknetlibfilepath':darknetlibfilepath,
                'datafilepath':datafilepath, 'cfgfilepath':cfgfilepath,
                'weightfilepath':weightfilepath}

        # for Server
        testvideo = config.get('Server', 'testvideo')

        serverc = {'testvideo':testvideo}

        # for Sql
        autocommit = config.getboolean('Sql', 'autocommit')
        host = config.get('Sql', 'host')
        port = config.getint('Sql', 'port')
        database = config.get('Sql', 'database')
        user = config.get('Sql', 'user')
        password = config.get('Sql', 'password')
        charset = config.get('Sql', 'charset')

        sqlc = {'autocommit':autocommit, 'host':host, 'port':port,
                'database':database, 'user':user, 'password':password,
                'charset':charset}

        return yoloc, serverc, sqlc
    except configparser.Error as config_parse_err:
        raise config_parse_err

def checker(logger, yolocfg, sqlc, serverc):
    try:
        logger.info("loading darknet-detector")
        darknet = Darknet(libfilepath=yolocfg['darknetlibfilepath'],
                      cfgfilepath=yolocfg['cfgfilepath'].encode(),
                      weightsfilepath=yolocfg['weightfilepath'].encode(),
                      datafilepath=yolocfg['datafilepath'].encode())
        darknet.load_conf()

        logger.info("checking DB connecter") # put your DB conn function
        db = Dbcc(host=sqlc['host'], port=sqlc['port'], database=sqlc['database'],
                  user=sqlc['user'], password=sqlc['password'])
        isconn = db.chk_db()
        if(isconn):
            logger.info("Database Connected")
        else:
            logger.error("Database Connect Error")
            return False
        
        logger.info("checking GPX module")
        points = getpoints(serverc['testvideo'])
        if len(points)>0:
            logger.info("GPX module Okay~")
        else:
            logger.error("GPX module failed")
            return False
            
        return True, darknet, db
    except:
        type, message, traceback = sys.exc_info()
        logger.error('!!!--->error<---!!!')
        logger.error(type)
        logger.error(message)
        logger.error('function|module-->', traceback.tb_frame.f_code.co_name)
        logger.error('file-->', traceback.tb_frame.f_code.co_filename)
        traceback = traceback.tb_next
        logger.error('!!!--->error-traceback-end<---!!!')
    
def main():
    servercfgpath = "./cfg/server.ini"
    logger = initlog()
    yoloc, serverc, sqlc = get_params(servercfgpath)
    logger.debug(yoloc)
    logger.debug(serverc)
    logger.debug(sqlc)
    logger.info("init the tra detector server")
    runflg, darknet, db = checker(logger, yoloc, sqlc, serverc)
    if(runflg):
        srv = Server(darknet, logger, db)
        logger.info("Server Start")
        srv.run()

if __name__ == '__main__':
    main()