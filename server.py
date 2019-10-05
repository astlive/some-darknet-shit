#pip lib
import os
import datetime
import logging
import multiprocessing as mp

#local module
from thirdc import getpoints
import dbcc

class Server:
    def __init__(self, darknet, logger, dbcc):
        self.detector = darknet
        self.logger = logger
        self.db = dbcc
        self.jobs = mp.Queue()
    
    def porter(self):
        sql = 'SELECT * FROM file WHERE file.status = 0'
        rr = self.db.query(sql)
        self.logger.info("Query the undetect record from DB num:" + str(len(rr)))
        for r in rr:
            self.logger.debug("r in rr-->" + str(r))
            job = {'fid':r[0],'fpath':r[2]}
            self.logger.debug("job-->" + str(job))
            self.jobs.put(job)
            
    def run(self):
        # self.porter()
        mpPorter = mp.Process(target=self.porter,args=(self,))
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
        
def main():
    logger = initlog()
    db = dbcc.dbcc()
    logger.info("init the tra detector server")
    if(checker(logger, db)):
        srv = Server("darknet", logger, db)
        logger.info("Server Start")
        srv.run()

if __name__ == '__main__':
    main()