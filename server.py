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
import signal
from PIL import Image
from pathlib import Path
import multiprocessing as mp

#local module
import darknet
from thirdc import getpoints
from dbcc import Dbcc

class Server:
    def __init__(self, yolocfg, logger, dbcc, srvc):
        self.yoloc = yolocfg
        self.altNames = None
        self.metaMain = None
        self.netMain = None
        self.logger = logger
        self.db = dbcc
        self.up_folder = srvc['filepath']
        self.jobs = mp.Manager().Queue()
        self.imgs = mp.Manager().Queue()
        self.pools = mp.Value('i', 0)
    
    def porter(self):
        self.logger.info("PID:" + str(os.getpid()) + " porter start")
        while True:
            rr = self.db.get_job()
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
                    self.db.updatefilestatus(0.000001, job['fid'])
                    self.jobs.put(job)
    
    def convertBack(self, x, y, w, h):
        xmin = int(round(x - (w / 2)))
        xmax = int(round(x + (w / 2)))
        ymin = int(round(y - (h / 2)))
        ymax = int(round(y + (h / 2)))
        return xmin, ymin, xmax, ymax


    def cvDrawBoxes(self, detections, img):
        imcaption = []
        for detection in detections:
            x, y, w, h = detection[2][0],\
                detection[2][1],\
                detection[2][2],\
                detection[2][3]
            xmin, ymin, xmax, ymax = self.convertBack(
                float(x), float(y), float(w), float(h))
            pt1 = (xmin, ymin)
            pt2 = (xmax, ymax)
            cv2.rectangle(img, pt1, pt2, (0, 255, 0), 1)
            cv2.putText(img,
                        detection[0].decode() +
                        " [" + str(round(detection[1] * 100, 2)) + "]",
                        (pt1[0], pt1[1] - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                        [0, 255, 0], 2)
            
            #code for get obj label and score
            label = detection[0].decode("ascii")
            confidence = detection[1]
            self.logger.debug(label+": "+str(np.rint(100 * confidence))+"%")
            obj = {'class_index':detection[3], 'obj_name':label, 'score': confidence}
            imcaption.append(obj)
        return img, imcaption

    def darkneter(self):
        self.logger.info("PID:" + str(os.getpid()) + " loading darknet-detector")
        
        configPath = self.yoloc['cfgfilepath'].encode()
        weightPath = self.yoloc['weightfilepath'].encode()
        metaPath = self.yoloc['datafilepath'].encode()

        self.netMain = darknet.load_net_custom(configPath, weightPath, 0, 1)  # batch size = 1
        self.metaMain = darknet.load_meta(metaPath)
        try:
            with open(metaPath) as metaFH:
                metaContents = metaFH.read()
                import re
                match = re.search("names *= *(.*)$", metaContents,
                                re.IGNORECASE | re.MULTILINE)
                if match:
                    result = match.group(1)
                else:
                    result = None
                try:
                    if os.path.exists(result):
                        with open(result) as namesFH:
                            namesList = namesFH.read().strip().split("\n")
                            self.altNames = [x.strip() for x in namesList]
                except TypeError:
                    pass
        except Exception:
            pass
        darknet_image = darknet.make_image(darknet.network_width(self.netMain),
                                    darknet.network_height(self.netMain),3)
        self.logger.info("....detector loading done.")

        while True:
            if(self.imgs.empty()):
                time.sleep(10)
            else:
                try:
                    img = self.imgs.get(False)
                    if('endflag' in img and img['endflag'] == True):
                        self.logger.info("Fid:" + str(img['fid']) + " Job done")
                        #some code to mark the record is end at database
                        self.db.updatefilestatus(1 ,img['fid'])
                    else:
                        CVmatimage = img['img_data']
                        frame_rgb = cv2.cvtColor(CVmatimage, cv2.COLOR_BGR2RGB)
                        frame_resized = cv2.resize(frame_rgb,
                                                    (darknet.network_width(self.netMain),
                                                    darknet.network_height(self.netMain)),
                                                    interpolation=cv2.INTER_LINEAR)
                        darknet.copy_image_from_bytes(darknet_image,frame_resized.tobytes())
                        detections = darknet.detect_image(self.netMain, self.metaMain, darknet_image, thresh=0.25)
                        
                        d_image, imcaption = self.cvDrawBoxes(detections, frame_resized)
                        d_image = cv2.cvtColor(d_image, cv2.COLOR_BGR2RGB)
                        cv2.imwrite(img['dimg_path'], d_image)
                        self.logger.debug("darknet.detect --> " + img['img_path'] + " has " + str(len(imcaption)) + " obj")
                        img['resultlist'] = imcaption
                        self.db.insertresult(img)
                        self.db.updatefilestatus(img['status'], img['fid'])
                        self.logger.info("job --> " + str(img['fid']) + " status:" + str(img['status']))
                except Exception as err:
                    raise err

    def img_worker(self, fpath, fid):
        self.logger.info("PID:" + str(os.getpid()) + " fid = " + str(fid)  + " img_worker target = " + str(fpath))
        try:
            with open(fpath, "rb") as inputfile:
                img_data = inputfile.read()
                img_np_arr = np.array(Image.open(io.BytesIO(img_data)).convert("RGB"))
                img = {'fid':fid ,'img_data':img_np_arr, 'endflag':True, 'isframe':0}
                self.imgs.put(img)
        except Exception as err:
            raise err
        os.kill(os.getpid(), signal.SIGTERM)

    def video_worker(self, fpath, fid):
        self.logger.info("PID:" + str(os.getpid()) + " fid = " + str(fid) +  " video_worker target = " + str(fpath))
        self.db.updatefilejustUpload(0, fid)
        try:
            points = getpoints(fpath)
            self.logger.info(str(fid) + " has " + str(len(points)) + " GPS point")
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
                        tmpp = os.path.join(ppath, (str(pcount) + "-" + str(i) + ".jpg"))
                        tmpdp = os.path.join(ppath, (str(pcount) + "-" + str(i) + "d.jpg"))
                        cv2.imwrite(tmpp, img)
                        uimg = {'fid':fid,'img_data':img, 'lat':curpoint.latitude, 'lon':curpoint.longitude, 'speed':curpoint.speed,
                        'UTCTIME':curpoint.time.strftime("%Y-%m-%d %H:%M:%S"), 'VIDEOTIME':(i/vframerate),
                        'status':(i/totalframe), 'img_path':tmpp, 'dimg_path':tmpdp, 'isframe':1}
                        self.imgs.put(uimg)
                        # self.logger.debug(uimg)
                        self.logger.debug("fid:" + str(fid) + " save to " + str(tmpp))
                    else:
                        self.logger.error("fid: " + str(fid) + " Read error at frame:" + str(i))
                endflag = {'fid':fid,'endflag':True}
                self.imgs.put(endflag)
                self.logger.debug(str(fid) + " endflag sended")
                vcap.release()
            else:
                self.logger.error("GPS data Miss or not a gopro video " + fpath)
                #code to mark the file do not detect
                self.db.updatefilejustUpload(1, fid)
        except Exception as err:
            raise err
        with(self.pools.get_lock()):
            self.pools.value = self.pools.value - 1
        os.kill(os.getpid(), signal.SIGTERM)

    def run(self):
        self.logger.info("PID:" + str(os.getpid()) + " Server Run")
        mpporter = mp.Process(target=self.porter)
        mpporter.start()
        mpdarknet = mp.Process(target=self.darkneter)
        mpdarknet.start()
        #to make sure darknet loaded yolov3 weight on sn750 take about 10(s)
        time.sleep(20)

        while True:
            self.logger.info("jobs:" + str(self.jobs.qsize()) + " imgs:" + str(self.imgs.qsize()) + " pools:" + str(self.pools.value))
            if self.jobs.empty() or self.pools.value >= 5:
                time.sleep(10)
            else:
                job = self.jobs.get()
                self.logger.debug("job --> " + str(job))
                jobfpath = os.path.join(self.up_folder,job['fpath'])
                if(job['type'] == 0):
                    with self.pools.get_lock():
                        self.pools.value = self.pools.value + 1
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
    verb.setLevel('INFO')
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
        datafilepath = config.get('Yolo', 'datafilepath')
        cfgfilepath = config.get('Yolo', 'cfgfilepath')
        weightfilepath = config.get('Yolo', 'weightfilepath')

        yoloc = {'datafilepath':datafilepath, 'cfgfilepath':cfgfilepath,
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
        logger.debug("call Server Start")
        srv.run()

if __name__ == '__main__':
    main()
