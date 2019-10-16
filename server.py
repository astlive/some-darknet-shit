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
    def __init__(self, yolocfg, logger, dbcc, srvc):
        self.yoloc = yolocfg
        self.logger = logger
        self.db = dbcc
        self.up_folder = srvc['filepath']
        self.jobs = mp.Manager().Queue()
        self.imgs = mp.Manager().Queue()
    
    def porter(self):
        while True:
            rr = self.db.get_job()
            self.logger.info("Query the undetect record from DB num:" + str(len(rr)))
            if(len(rr) == 0):
                time.sleep(10)
            else:
                for r in rr:
                    self.logger.debug("r in rr-->" + str(r))
                    job = {'fid':r[0],'fpath':r[2]}
                    if(Path(r[2]).suffix == ".mp4"):
                        job['type'] = 0
                    else:
                        job['type'] = 1
                    self.logger.debug("job-->" + str(job))
                    self.jobs.put(job)
                    self.db.updatefilestatus(2, job['fid'])
    
    def darkneter(self):
        self.logger.debug(str(os.getpid()) + " darkneter")
        self.logger.info("loading darknet-detector")
        darknet = Darknet(libfilepath=self.yoloc['darknetlibfilepath'],
                      cfgfilepath=self.yoloc['cfgfilepath'].encode(),
                      weightsfilepath=self.yoloc['weightfilepath'].encode(),
                      datafilepath=self.yoloc['datafilepath'].encode())
        darknet.load_conf()
        self.logger.info("....detector loading done.")
        while True:
            if(self.imgs.empty()):
                time.sleep(10)
            else:
                try:
                    img = self.imgs.get(False)
                    if('endflag' in img and img['endflag'] == True):
                        self.logger.info("Fid:" + str(img['fid']) + " Job done")
                        self.logger.debug("imgs-->qsize() " + str(self.imgs.qsize()))
                        #some code to mark the record is end at database
                    else:
                        #thresh --> 0.5
                        yolo_results = darknet.detect(img['img_data'], 0.5)
                        for yolo_result in yolo_results:
                            self.logger.debug(yolo_result.get_detect_result())
                except Exception as err:
                    raise err
                pass

    def img_worker(self, fpath, fid):
        self.logger.debug(str(os.getpid()) + " fid = " + str(fid)  + " img_worker target = " + str(fpath))
        try:
            with open(fpath, "rb") as inputfile:
                img_data = inputfile.read()
                img_np_arr = np.array(Image.open(io.BytesIO(img_data)).convert("RGB"))
                img = {'fid':fid ,'img_data':img_np_arr, 'endflag':True}
                self.imgs.put(img)
        except Exception as err:
            raise err
        pass

    def video_worker(self, fpath, fid):
        self.logger.debug(str(os.getpid()) + " fid = " + str(fid) +  " video_worker target = " + str(fpath))
        try:
            points = getpoints(fpath)
            if(len(points) > 0):
                ppath = os.path.join(Path(fpath).parent,"video",os.path.basename(fpath))
                for i in range(100):
                    if(os.path.exists(ppath)):
                        ppath = os.path.join(ppath,str(i))
                    else:
                        break
                self.logger.debug("fid: " + str(fid) + " Create dir to put images at " + str(ppath))
                os.makedirs(ppath)
                vcap = cv2.VideoCapture(fpath)
                vframerate = vcap.get(cv2.CAP_PROP_FPS)
                totalframe = round(vcap.get(cv2.CAP_PROP_FRAME_COUNT))
                vcaprate = round(vframerate/6)
                pcount = 0
                curpoint = points[pcount]
                self.logger.debug("fid: " + str(fid) + " totalframe:" + str(totalframe) + " vframerate:" + str(vframerate) + " vcaprate:" + str(vcaprate))
                for i in range(0, totalframe, vcaprate):
                    if(i % vcaprate == 0):
                        if(i % (vcaprate*3) == 0 and i % (vcaprate*6) != 0 and i != 0 and pcount < len(points)-1):
                            pcount = pcount + 1
                            curpoint = points[pcount]
                    vcap.set(cv2.CAP_PROP_POS_FRAMES, i)
                    success, img = vcap.read()
                    if(success and img is not None):
                        uimg = {'fid':fid,'img_data':img, 'point':curpoint}
                        self.imgs.put(uimg)
                        self.logger.debug(uimg)
                        tmpp = os.path.join(ppath, (str(pcount) + "-" + str(i) + ".jpg"))
                        cv2.imwrite(tmpp, img)
                        self.logger.debug("fid:" + str(fid) + " imwrite to " + str(tmpp))
                    else:
                        self.logger.error("fid: " + str(fid) + " Read error at frame:" + str(i))
                endflag = {'fid':fid,'endflag':True}
                self.imgs.put(endflag)
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
                if(job['type'] == 0):
                    mp.Process(target=self.video_worker, args=(jobfpath,job['fid'],)).start()
                    self.logger.info("Video job " + str(jobfpath) + " Fid:" + str(job['fid']) + " Start")
                elif(job['type'] == 1):
                    mp.Process(target=self.img_worker, args=(jobfpath,job['fid'],)).start()
                    self.logger.info("image job " + str(jobfpath) + " Fid:" + str(job['fid']) + " Start")

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

def checker(logger, sqlc, serverc):
    try:
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
            
        return True, db
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
    logger.info("init the TRA detector server")
    runflg, db = checker(logger, sqlc, serverc)
    if(runflg):
        srv = Server(yoloc, logger, db, serverc)
        logger.info("Server Start")
        srv.run()

if __name__ == '__main__':
    main()
