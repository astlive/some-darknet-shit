#pip lib
import sys
import os
import time
import datetime
import logging
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
            sql = 'SELECT * FROM file WHERE file.status = 0'
            rr = self.db.query(sql)
            self.logger.info("Query the undetect record from DB num:" + str(len(rr)))
            if(len(rr) is 0):
                time.sleep(10)
            else:
                for r in rr:
                    self.logger.debug("r in rr-->" + str(r))
                    job = {'fid':r[0],'fpath':r[2]}
                    self.logger.debug("job-->" + str(job))
                    self.jobs.put(job)
                    self.db.updatefilestatus(2, job['fid'])
                
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

def checker(logger, dbcc):
    try:
        logger.info("checking darknet-detector")
        logger.info("checking DB connecter") # Here to put your DB conn function
        isconn = dbcc.chk_db()
        if(isconn):
            logger.info("Database Connected")
        else:
            logger.error("Database Connect Error")
            return False
        logger.info("checking GPX module")
        points = getpoints(os.path.join(os.getcwd(), "test.mp4"))
        if len(points)>0:
            logger.info("GPX module Okay~")
        else:
            logger.error("GPX module failed")
            return False
        return True
    except:
        type, message, traceback = sys.exc_info()
        print('!!!--->error<---!!!')
        print(type)
        print(message)
        print('function|module-->', traceback.tb_frame.f_code.co_name)
        print('file-->', traceback.tb_frame.f_code.co_filename)
        traceback = traceback.tb_next
        print('!!!--->error-traceback-end<---!!!')
    
def main():
    logger = initlog()
    db = Dbcc()
    logger.info("init the tra detector server")
    if(checker(logger, db)):
        srv = Server("darknet", logger, db)
        logger.info("Server Start")
        srv.run()

if __name__ == '__main__':
    main()