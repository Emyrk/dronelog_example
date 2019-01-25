#!/usr/bin/env python2
import csv
import json
from factom import Factomd, FactomWalletd
import pdb
import random
from utils import hex
import sys

# Grabbed from python factomd lib
def new_entry(walletd, factomd, chain_id, ext_ids, content, ec_address=None):
        """
        Shortcut method to create a new entry.

        Args:
            factomd (Factomd): The `Factomd` instance where the creation
                message will be submitted.
            chain_id (str): Chain ID where entry will be appended.
            ext_ids (list[str]): A list of external IDs, unencoded.
            content (str): Entry content, unencoded.
            ec_address (str): Entry credit address to pay with. If not
                provided `self.ec_address` will be used.

        Returns:
            dict: API result from the final `reveal_chain()` call.
        """
        calls = walletd._request('compose-entry', {
            'entry': {
                'chainid': chain_id,
                'extids': hex(ext_ids),
                'content': hex(content)
            },
            'ecpub': ec_address or walletd.ec_address
        })
        factomd.commit_entry(calls['commit']['params']['message'])
        return factomd.reveal_entry(calls['reveal']['params']['entry'])



# Some fields we do not need
def removeFields(fields, the_dict):
    for key in fields:
    	if(fields[key][0] == 0):
	        if key in the_dict:
	            del the_dict[key]

# Save space by renaming
def renameFields(fields, the_dict):
	for old_key in fields:
		if(fields[old_key][2] != ""):
			if old_key in the_dict:
				the_dict[fields[old_key][2]] = the_dict.pop(old_key)

# Convert some fields from strings to their actual value
def convertFields(fields, the_dict):
	for key in fields:
		if(fields[key][1] != None):
			if key in the_dict:
				the_dict[key] = fields[key][1](the_dict[key])


if len(sys.argv) < 2:
	print("./factomize LOGFILE")
	sys.exit()

csvfile = open(sys.argv[1], 'r')

currentBin = []
dataBins = []
windowSize = 10.0 # In seconds
window = windowSize


fieldnames = ("Id", "Time(seconds)", "Time(text)", "Latitude", "Longitude", "FlightMode", "Altitude(feet)", "Altitude(meters)", "VpsAltitude(feet)", "VpsAltitude(meters)", "HSpeed(mph)", "HSpeed(m/s)", "GpsSpeed(mph)", "GpsSpeed(m/s)", "HomeDistance(feet)", "HomeDistance(meters)", "HomeLatitude", "HomeLongitude", "GpsCount", "GpsLevel", "BatteryPower(%)", "BatteryVoltage", "BatteryVoltageDeviation", "BatteryCell1Voltage", "BatteryCell2Voltage", "BatteryCell3Voltage", "BatteryCell4Voltage", "VelocityX", "VelocityY", "VelocityZ", "Pitch", "Roll", "Yaw", "Yaw(360)", "RcAileron", "RcElevator", "RcGyro", "RcRudder", "RcThrottle", "NonGpsError", "GoHomeStatus", "AppTip", "AppWarning", "AppMessage")
#(
# 0 == Remove
# Type
# Shortened name
#)
fieldprops = {
 "Id":							(1, int, ""),
 "Time(seconds)":				(1, float, "Time(s)"),
 "Altitude(meters)":			(1, float, "Alt(m)"),
 "Latitude":					(1, float, "Lat"),
 "Longitude":					(1, float, "Lng"),
 "BatteryPower(%)":				(1, float, "Bat"),


 "Time(text)":					(0, None, ""),
 "FlightMode":					(0, None, ""),
 "Altitude(feet)":				(0, None, ""),
 "VpsAltitude(feet)":			(0, None, ""),
 "VpsAltitude(meters)":			(0, None, "VpsAlt(m)"),
 "HSpeed(mph)":					(0, None, "HSpd(mph)"),
 "HSpeed(m/s)":					(0, None, ""),
 "GpsSpeed(mph)":				(0, None, ""),
 "GpsSpeed(m/s)":				(0, None, ""),
 "HomeDistance(feet)":			(0, None, ""),
 "HomeDistance(meters)":		(0, None, ""),
 "HomeLatitude":				(0, None, ""),
 "HomeLongitude":				(0, None, ""),
 "GpsCount":					(0, None, ""),
 "GpsLevel":					(0, None, ""),
 "BatteryVoltage":				(0, None, ""),
 "BatteryVoltageDeviation":		(0, None, ""),
 "BatteryCell1Voltage":			(0, None, ""),
 "BatteryCell2Voltage":			(0, None, ""),
 "BatteryCell3Voltage":			(0, None, ""),
 "BatteryCell4Voltage":			(0, None, ""),
 "VelocityX":					(0, None, ""),
 "VelocityY":					(0, None, ""),
 "VelocityZ":					(0, None, ""),
 "Pitch":						(0, None, ""),
 "Roll":						(0, None, ""),
 "Yaw":							(0, None, ""),
 "Yaw(360)":					(0, None, ""),
 "RcAileron":					(0, None, ""),
 "RcElevator":					(0, None, ""),
 "RcGyro":						(0, None, ""),
 "RcRudder":					(0, None, ""),
 "RcThrottle":					(0, None, ""),
 "NonGpsError":					(0, None, ""),
 "GoHomeStatus":				(0, None, ""),
 "AppTip":						(0, None, ""),
 "AppWarning":					(0, None, ""),
 "AppMessage":					(0, None, ""),
}



reader = csv.DictReader(csvfile, fieldnames)
i = -1
for row in reader:
	i = i + 1
	if i == 0:
		continue

	removeFields(fieldprops, row)
	convertFields(fieldprops, row)
	renameFields(fieldprops, row)

	j = json.dumps(row)
	v = json.loads(j)
	# pdb.set_trace()
	if(float(v[fieldprops['Time(seconds)'][2]]) < window):
		currentBin.append(v)
	else:
		window = window + windowSize
		dataBins.append(currentBin[:])
		currentBin = [v]

dataBins.append(currentBin)

# Write to files to store logs locally
for data in dataBins:
	datafile = open('databins/data' + str(i), 'w')
	json.dump(data, datafile, separators=(',', ':'))

chain_id = '35500118f038279a79d7b6a07c7736c6a120332e6866069b558ae71b0617e19e'
ec_address = 'EC3gSYeXW1cBbY6u4qDod5Zqrd6u1dDuM5TpMxnkfzbcDQwUKmUV'
walletd = FactomWalletd()
factomd = Factomd()

entryhashes = []
# Write files to blockchain
for data in dataBins:
	content = json.dumps(data, separators=(',', ':'))
	resp = new_entry(walletd, factomd, chain_id, ['Drone-log', str(random.randint(0, 900000))], content, ec_address=ec_address)
	entryhashes.append(resp['entryhash'])
	print(resp['entryhash'])

for entryhash in entryhashes:
	print(entryhash)



