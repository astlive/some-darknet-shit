#pip lib
import sys
import os
import time
import datetime
import logging
import configparser
import numpy as np
import io
import cv2
from PIL import Image
from pathlib import Path
import multiprocessing as mp

#local module
from darknet import Darknet
from thirdc import getpoints
from dbcc import Dbcc

class Server:
    def __init__(self, darknet, logger, dbcc, srvc):
        self.detector = darknet
        self.logger = logger
        self.db = dbcc
        self.up_folder = srvc['filepath']
        self.jobs = mp.Queue()
        self.imgs = mp.Queue()
    
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
    
    def darkneter(self):
        while True:
            if(self.imgs.empty()):
                time.sleep(10)
            else:
                pass

    def img_worker(self, fpath, fid):
        self.logger.debug(os.getpid() + " fid = " + fid  + " img_worker target = " + fpath)
        try:
            with open(fpath, "rb") as inputfile:
                img_data = inputfile.read()
                img_np_arr = np.array(Image.open(io.BytesIO(img_data)).convert("RGB"))
                img = {'fid':fid ,'img_data':img_np_arr}
                self.imgs.put(img)
        except Exception as err:
            raise err
        pass

    def video_worker(self, fpath, fid):
        self.logger.debug(os.getpid() + " fid = " + fid +  " video_worker target = " + fpath)
        try:
            points = getpoints(fpath)
            if(len(points) > 0):
                ppath = os.path.join(Path(fpath).parent,"/upload",os.path.basename(fpath))
                for i in range(100):
                    if(os.path.exists(ppath)):
                        ppath = os.path.join(ppath,str(i))
                    else:
                        break
                os.makedirs(ppath)
                self.logger.debug("fid: " + fid + " Create dir to put images at " + ppath)
                vcap = cv2.VideoCapture(fpath)
                vframerate = vcap.get(cv2.CAP_PROP_FPS)
                vcaprate = round(vframerate/6)
                pcount = 0
                curpoint = points[pcount]
                self.logger.debug("fid: " + fid + " vframerate:" + vframerate + " vcaprate:" + vcaprate)
                for i in range(0, vcap.get(cv2.CAP_PROP_FRAME_COUNT), vcaprate):
                    if(i % vcaprate is 0):
                        if(i % (vcaprate*3) is 0 and i is not 0):
                            pcount = pcount + 1
                            curpoint = points[pcount]
                    vcap.set(cv2.CAP_PROP_POS_FRAME, i)
                    success, img = vcap.read()
                    if(success and img is not None):
                        uimg = {'fid':fid,'img_data':img}
                        self.imgs.put(uimg)
                        self.logger.debug(uimg)
                        tmpp = os.path.join(ppath, str(pcount) + "-", str(i) + ".jpg")
                        cv2.imwrite(tmpp, img)
                        self.logger.debug("fid:" + fid + " imwrite to " + tmpp)
                    else:
                        self.logger.error("fid: " + fid + " Read error at frame:" + str(i))
            else:
                self.logger.error("GPS data Miss or not a gopro video " + fpath)
                #some code to mark the record is not a valid GoPro video at database and exit process
        except Exception as err:
            raise err

    def run(self):
        mpporter = mp.Process(target=self.porter)
        mpporter.start()
        mpdarknet = mp.Process(target=self.darkneter)
        mpdarknet.start()

        while True:
            if self.jobs.empty():
                time.sleep(10)
            else:
                job = self.jobs.get()
                self.logger.debug(job)
                jobfpath = os.path.join(self.up_folder,job['fpath'])
                if(job['type'] is 0):
                    mp.Process(target=self.video_worker, args=(jobfpath,job['fid'],)).start()
                    self.logger.info("Video job" + jobfpath + " Fid:" + job['fid'] + " Start")
                elif(job['type'] is 1):
                    mp.Process(target=self.img_worker, args=(jobfpath,job['fid'],)).start()
                    self.logger.info("image job" + jobfpath + " Fid:" + job['fid'] + " Start")

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
        filepath = config.get('Server', 'filepath')

        serverc = {'testvideo':testvideo, 'filepath':filepath}

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
        srv = Server(darknet, logger, db, serverc)
        logger.info("Server Start")
        srv.run()

if __name__ == '__main__':
    main()
