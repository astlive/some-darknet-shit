#pip lib
import os
import datetime
import logging

#local module
from thirdc import getpoints
import dbcc

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

def checker(logger):
    logger.info("checking darknet-detector")
    logger.info("checking DB connecter") # Here to put your DB conn
    db = dbcc()
    isconn, ping = db.chk_db()
    if(isconn):
        logger.info("Database Connected Ping: ", ping)
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
        
def main():
    logger = initlog()
    logger.info("Tra Darknet Service start")
    

if __name__ == '__main__':
    main()