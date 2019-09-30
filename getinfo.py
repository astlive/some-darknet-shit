from pathlib import Path
import json
import argparse
import subprocess
import re
import struct
import os
import platform
from collections import namedtuple
import array
import sys
import time
from datetime import datetime
import cv2

import gopro2gpx
import config
import gpmf
import fourCC
import time
import sys

import gpshelper

def args_reciver():
	parser = argparse.ArgumentParser()
	parser.add_argument("GPV", help="Path to GoPro Video")
	parser.add_argument("-T", "--test", help="Test mode 1", action="store_true")
	return parser.parse_args()

def main(config, args, tflag):
	parser = gpmf.Parser(config)
	data = parser.readFromMP4()
	print("skip bad GPS points:" + str(args.skip))
	points = gopro2gpx.BuildGPSPoints(data, skip=args.skip)
	
	if(len(points)==0):
		print("Can't create file. No GPS info in %s. Exitting" % args.file)
		sys.exit(0)
	
	for pp in points:
		print("Time:", pp.time, " lat:", pp.latitude, " lon:", pp.longitude, "speed:",pp.speed)
	# gpx = gpshelper.generate_GPX(points, trk_name="gopro7-track")
	# print(gpx)

def getinfo(vpath):
	import config
	dcfg = argparse.Namespace(**{'binary':False, 'file':vpath, 'outputfile':vpath, 'skip':True, 'verbose':0})
	config = config.setup_environment(dcfg)
	parser = gpmf.Parser(config)
	data = parser.readFromMP4()
	points = gopro2gpx.BuildGPSPoints(data, skip=True)
	return points

def splitmp4withmpoint(lenpoints, vfile, ppath):
	# print("splitmp4withmpoint.vfile:", vfile)
	vcap = cv2.VideoCapture(vfile)
	video_frame_rate = vcap.get(cv2.CAP_PROP_FPS)
	capcount = round(video_frame_rate/12)
	count = 0
	pcount = 0
	scount = 1
	flist = []
	record = {}
	# print("frame count:", vcap.get(cv2.CAP_PROP_FRAME_COUNT),"Video frame rate:", video_frame_rate, "cap rate:", capcount, "/frame")
	success, image = vcap.read()
	# pp = points[pcount]
	# print("Time:", pp.time, " lat:", pp.latitude, " lon:", pp.longitude, "speed:",pp.speed)
	while count < vcap.get(cv2.CAP_PROP_FRAME_COUNT):
		if(count%capcount==0 and image is not None):
			tmpp = os.path.join(ppath, str(pcount) + "-" + str(count) + '.jpg')
			cv2.imwrite(tmpp, image)
			record = {'p':pcount, 'fs':count/video_frame_rate, 'path':tmpp}
			flist.append(record)
		if(count > 0 and count%(6*capcount)==0):
			if(scount==1 and (pcount+1)<lenpoints):
				pcount+=1
				# pp=points[pcount]
				# print("Time:", pp.time, " lat:", pp.latitude, " lon:", pp.longitude, "speed:",pp.speed)
				scount=0
			else:
				scount=1
		count+=1
		success, image = vcap.read()
		# if(not success):
		# 	print("Fatel Error at frame:", count)
	return flist
	

if __name__ == "__main__":
	args = args_reciver()
	if(Path(args.GPV).is_file()):
		default = {'binary':False, 'file':args.GPV, 'outputfile':args.GPV, 'skip':True, 'verbose':1}
		defcfg = argparse.Namespace(**default)
		config = config.setup_environment(defcfg)
		main(config, defcfg, args.test)
	else:
		print(args.GPV + " does not exist or not a file")
		