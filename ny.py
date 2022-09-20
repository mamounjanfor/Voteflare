import json
import os
import csv
import sys
import string
import shutil
from zipfile import ZipFile
from os.path import basename
import random
import datetime


# source:

def cleanstring(instr):
    newstr = ""
    for ch in instr:
        if ch.isalnum() or (ch == " "):
            newstr = newstr + ch
    return newstr


def parsecsvrow(aline):
    results = {}
    results["_lastname"] = ""
    results["_firstname"] = ""
    results["_middlename"] = ""
    results["_namesuffix"] = ""
    results["_raddnumber"] = ""
    results["_rhalfcode"] = ""
    results["_rpredirection"] = ""
    results["_rstreetname"] = ""
    results["_rpostdirection"] = ""
    results["_rapartmenttype"] = ""
    results["_rapartment"] = ""
    results["_raddrnonstd"] = ""
    results["_rcity"] = ""
    results["_rzip5"] = ""
    results["_rzip4"] = ""
    results["_mailadd1"] = ""
    results["_mailadd2"] = ""
    results["_mailadd3"] = ""
    results["_mailadd4"] = ""
    results["_dob"] = ""
    results["_gender"] = ""
    results["_enrollment"] = ""
    results["_otherparty"] = ""
    results["_countycode"] = ""
    results["_ed"] = ""
    results["_ld"] = ""
    results["_towncity"] = ""
    results["_ward"] = ""
    results["_cd"] = ""
    results["_sd"] = ""
    results["_ad"] = ""
    results["_lastvoterdate"] = ""
    results["_prevyearvoted"] = ""
    results["_prevcounty"] = ""
    results["_prevaddress"] = ""
    results["_prevname"] = ""
    results["_countyvrnumber"] = ""
    results["_regdate"] = ""
    results["_vrsource"] = ""
    results["_idrequired"] = ""
    results["_idmet"] = ""
    results["_status"] = ""
    results["_reasoncode"] = ""
    results["_inact_date"] = ""
    results["_purge_date"] = ""
    results["_sboeid"] = ""
    results["_voterhistory"] = ""

    results["_lastname"] = aline[0].strip()
    results["_firstname"] = aline[1].strip()
    results["_middlename"] = aline[2].strip()
    results["_namesuffix"] = aline[3].strip()
    results["_raddnumber"] = aline[4].strip()
    results["_rhalfcode"] = aline[5].strip()
    results["_rpredirection"] = aline[6].strip()
    results["_rstreetname"] = aline[7].strip()
    results["_rpostdirection"] = aline[8].strip()
    results["_rapartmenttype"] = aline[9].strip()
    results["_rapartment"] = aline[10].strip()
    results["_raddrnonstd"] = aline[11].strip()
    results["_rcity"] = aline[12].strip()
    results["_rzip5"] = aline[13].strip()
    results["_rzip4"] = aline[14].strip()
    results["_mailadd1"] = aline[15].strip()
    results["_mailadd2"] = aline[16].strip()
    results["_mailadd3"] = aline[17].strip()
    results["_mailadd4"] = aline[18].strip()
    results["_dob"] = aline[19].strip()
    results["_gender"] = aline[20].strip()
    results["_enrollment"] = aline[21].strip()
    results["_otherparty"] = aline[22].strip()
    results["_countycode"] = aline[23].strip()
    results["_ed"] = aline[24].strip()
    results["_ld"] = aline[25].strip()
    results["_towncity"] = aline[26].strip()
    results["_ward"] = aline[27].strip()
    results["_cd"] = aline[28].strip()
    results["_sd"] = aline[29].strip()
    results["_ad"] = aline[30].strip()
    results["_lastvoterdate"] = aline[31].strip()
    results["_prevyearvoted"] = aline[32].strip()
    results["_prevcounty"] = aline[33].strip()
    results["_prevaddress"] = aline[34].strip()
    results["_prevname"] = aline[35].strip()
    results["_countyvrnumber"] = aline[36].strip()
    results["_regdate"] = aline[37].strip()
    results["_vrsource"] = aline[38].strip()
    results["_idrequired"] = aline[39].strip()
    results["_idmet"] = aline[40].strip()
    results["_status"] = aline[41].strip()
    results["_reasoncode"] = aline[42].strip()
    results["_inact_date"] = aline[43].strip()
    results["_purge_date"] = aline[44].strip()
    results["_sboeid"] = aline[45].strip()
    results["_voterhistory"] = aline[46].strip()

    # following field changes match those retrieved on lookup
    results["state2letter"] = ""
    results["lastname"] = ""
    results["firstname"] = ""
    results["zip5"] = ""
    results["birthdate_yyyymmdd"] = ""
    results["voter_county"] = ""
    results["voter_status"] = ""
    results["registrationinfo"] = ""
    results["precinctinfo"] = ""
    results["partyinfo"] = ""
    results["nameinfo"] = ""
    results["ageinfo"] = ""
    results["addressinfo"] = ""
    results["voterinfo"] = ""
    results["stateinfo"] = ""
    results["datetime"] = ""

    results["state2letter"] = "NY"
    results["lastname"] = results["_lastname"].strip()
    results["firstname"] = results["_firstname"].strip()
    results["zip5"] = results["_rzip5"]
    dob = results["_dob"]
    doby = dob[0:4]
    dobm = dob[4:6]
    dobd = dob[6:]
    if int(doby) < 1917:
        results["birthdate_yyyymmdd"] = "00000000"
    else:
        results["birthdate_yyyymmdd"] = doby + dobm + dobd

    ny_counties = {"1": "Albany",
                   "2": "Allegany",
                   "3": "Bronx",
                   "4": "Broome",
                   "5": "Cattaraugus",
                   "6": "Cayuga",
                   "7": "Chautauqua",
                   "8": "Chemung",
                   "9": "Chenango",
                   "10": "Clinton",
                   "11": "Columbia",
                   "12": "Cortland",
                   "13": "Delaware",
                   "14": "Dutchess",
                   "15": "Erie",
                   "16": "Essex",
                   "17": "Franklin",
                   "18": "Fulton",
                   "19": "Genesee",
                   "20": "Greene",
                   "21": "Hamilton",
                   "22": "Herkimer",
                   "23": "Jefferson",
                   "24": "Kings",
                   "25": "Lewis",
                   "26": "Livingston",
                   "27": "Madison",
                   "28": "Monroe",
                   "29": "Montgomery",
                   "30": "Nassau",
                   "31": "New York",
                   "32": "Niagara",
                   "33": "Oneida",
                   "34": "Onondaga",
                   "35": "Ontario",
                   "36": "Orange",
                   "37": "Orleans",
                   "38": "Oswego",
                   "39": "Otsego",
                   "40": "Putnam",
                   "41": "Queens",
                   "42": "Rensselaer",
                   "43": "Richmond",
                   "44": "Rockland",
                   "45": "Saratoga",
                   "46": "Schenectady",
                   "47": "Schoharie",
                   "48": "Schuyler",
                   "49": "Seneca",
                   "50": "St.Lawrence",
                   "51": "Steuben",
                   "52": "Suffolk",
                   "53": "Sullivan",
                   "54": "Tioga",
                   "55": "Tompkins",
                   "56": "Ulster",
                   "57": "Warren",
                   "58": "Washington",
                   "59": "Wayne",
                   "60": "Westchester",
                   "61": "Wyoming",
                   "62": "Yates",

                   }
    votercounty = results["_countycode"]
    results["voter_county"] = ny_counties[votercounty]

    status = results["_status"]
    if status == "A":
        results["voter_status"] = "Active"
    if status == "AM":
        results["voter_status"] = "Active Military"
    if status == "AF":
        results["voter_status"] = "Active Special Federal"
    if status == "AP":
        results["voter_status"] = "Active Special Presidential"
    if status == "AU":
        results["voter_status"] = "Active UOCAVA"
    if status == "I":
        results["voter_status"] = "Inactive"
    if status == "P":
        results["voter_status"] = "Purged"
    if status == "17":
        results["voter_status"] = "Prereg – Older than 16 years but younger than 18 years"

    results["registrationinfo"] = "Registration date: " + results["_regdate"]
    results["precinctinfo"] = "Precinct code: " + results["_ed"]
    party = results["_enrollment"]
    otherparty = results["_otherparty"]
    if party == "DEM":
        results["partyinfo"] = "Democratic"
    if party == "REP":
        results["partyinfo"] = "Republican"
    if party == "CON":
        results["partyinfo"] = "Conservative"
    if party == "WOR":
        results["partyinfo"] = "Working Families"
    if party == "BLK":
        results["partyinfo"] = " No party affiliation designated"
    if party == "OTH":
        if otherparty == "GRE":
            results["partyinfo"] = "Green"
        if otherparty == "LBT":
            results["partyinfo"] = "Libertarian"
        if otherparty == "SAM":
            results["partyinfo"] = "Serve America Movement"
        if otherparty == "IND":
            results["partyinfo"] = "Independence"
        if otherparty == "WEP":
            results["partyinfo"] = "Women's Equality"
        if otherparty == "REF":
            results["partyinfo"] = "Reform"
        if otherparty == "OTH":
            results["partyinfo"] = "Other"
    results["nameinfo"] = "Name (First Middle Last Suffix):"
    if len(results["_firstname"].strip()) > 0:
        results["nameinfo"] = results["nameinfo"] + " " + results["_firstname"].strip()
    if len(results["_middlename"].strip()) > 0:
        results["nameinfo"] = results["nameinfo"] + " " + results["_middlename"].strip()
    if len(results["_lastname"].strip()) > 0:
        results["nameinfo"] = results["nameinfo"] + " " + results["_lastname"].strip()
    if len(results["_namesuffix"].strip()) > 0:
        results["nameinfo"] = results["nameinfo"] + " " + results["_namesuffix"].strip()

    results["ageinfo"] = "Date of birth (yyyymmdd): " + results["birthdate_yyyymmdd"]

    results["addressinfo"] = "Residential address:"
    if len(results["_raddnumber"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_raddnumber"].strip()
    if len(results["_rhalfcode"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rhalfcode"].strip() + ","
    if len(results["_rpredirection"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rpredirection"].strip()
    if len(results["_rstreetname"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rstreetname"].strip()
    if len(results["_rpostdirection"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rpostdirection"].strip()
    if len(results["_rapartmenttype"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rapartmenttype"].strip()
    if len(results["_raddrnonstd"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_raddrnonstd"].strip()
    if len(results["_rcity"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rcity"].strip() + ", NY"
    if len(results["_rzip5"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rzip5"].strip()
    if len(results["_rzip4"].strip()) > 0:
        results["addressinfo"] = results["addressinfo"] + " " + results["_rzip4"].strip()

    results["voterinfo"] = "Voter ID: " + results["_sboeid"]
    results["stateinfo"] = "NY"
    timestamp = "1662582365"  ### xxx 09/07/2022
    results["timestamp"] = timestamp
    timestr = datetime.datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    results["datetime"] = timestr + " UTC"
    return (results)


def randomword(length):
    letters = string.digits
    return ''.join(random.choice(letters) for i in range(length))


def processacsvfile(datafilename, resultsdirdob, resultsdirname):
    print("Working on file: " + datafilename)
    file = open(datafilename, encoding="ISO-8859-1")
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)
    count = 0
    for row in csvreader:
        count += 1
        if (count % 10000) == 0:
            print("processing " + str(count))

        results = parsecsvrow(row)
        newfilenamedob = "_" + results["state2letter"] + "_" + results["birthdate_yyyymmdd"] + "_" + cleanstring(
            results["firstname"]).upper() + "_" + cleanstring(results["lastname"]).upper() + "_" + results[
                             "zip5"] + "_" + cleanstring(results["voter_county"]).upper().replace(" ", "")
        newfilenamename = "_" + results["state2letter"] + "_" + cleanstring(
            results["firstname"]).upper() + "_" + cleanstring(results["lastname"]).upper() + "_" + results[
                              "zip5"] + "_" + cleanstring(results["voter_county"]).upper().replace(" ", "") + "_" + \
                          results["birthdate_yyyymmdd"]
        # old version - newfilename = "_" + results["birthdate_yyyymmdd"] + "_" + results["firstname"] + "_" + results["lastname"] + "_" + results["zip5"] + "_" + results["voter_state_2letter"] + "_" + results["voter_county"].replace(" ", "")
        text_file = open(resultsdirdob + "/" + newfilenamedob, "w")
        text_file.write(json.dumps(results))
        text_file.close()
        text_file = open(resultsdirname + "/" + newfilenamename, "w")
        text_file.write(json.dumps(results))
        text_file.close()

    print("Total of " + str(count) + " rows processed.")


def processafile(datafilename, resultsdir):
    print("Working on file: " + datafilename)
    count = 0
    rawfile = open(datafilename, 'r')
    while True:
        aline = rawfile.readline()
        if not aline:
            break
        count += 1
        if (count % 10000) == 0:
            print("processing " + str(count))
        results = parsecsvrow(aline)
        newfilename = "_" + results["zip5"] + "_" + results["birthdate_mmddyyyy"] + "_" + results["lastname"] + "_" + \
                      results["firstname"] + "_" + results["county_tx"]
        #        newfilename = "_" + results["county_tx"] + "_" + results["lastname"] + "_" + results["firstname"]
        text_file = open(resultsdir + "/" + newfilename, "w")
        text_file.write(json.dumps(results))
        text_file.close()
    rawfile.close()
    print("Total of " + str(count) + " files processed.")


def processfiles(startdir, prefix, resultsdir):
    print("Working on files in " + startdir)
    count = 0
    for root, dirs, files in os.walk(startdir, topdown=False):
        for name in files:
            if name[0] != ".":
                if name.startswith(prefix):
                    rawfile = open(startdir + "/" + name, 'r')
                    while True:
                        aline = rawfile.readline()
                        if not aline:
                            break
                        count += 1
                        if (count % 10000) == 0:
                            print("processing " + str(count))
                        results = parsecsvrow(aline)
                        newfilename = "_" + results["zip5"] + "_" + results["birthdate_mmddyyyy"] + "_" + results[
                            "lastname"] + "_" + results["firstname"] + "_" + results["county_tx"]
                        text_file = open(resultsdir + "/" + newfilename, "w")
                        text_file.write(json.dumps(results))
                        text_file.close()
                    rawfile.close()


def makezipfiles(startdir, resultsdir):
    print("Working on files in " + startdir)
    count = 0
    filecount = 1
    zipObj = ZipFile(resultsdir + "/" + "data" + str(filecount) + ".zip", 'w')
    for root, dirs, files in os.walk(startdir, topdown=False):
        for name in files:
            if name[0] != ".":
                count += 1
                if (count % 15000) == 0:
                    zipObj.close()
                    print("Finished " + resultsdir + "/" + "data" + str(filecount) + ".zip")
                    filecount += 1
                    zipObj = ZipFile(resultsdir + "/" + "data" + str(filecount) + ".zip", 'w')
                zipObj.write(startdir + "/" + name)
    zipObj.close()
    print("Finished " + resultsdir + "/" + "data" + str(filecount) + ".zip")
    print("Total of " + str(count) + " files processed.")


###################################################################################

### Results: each record is a JSON named: _dob_birthdate_yyyymmdd_firstname_lastname_zip5_voter_state_2letter_county

###
###################################################################################

#
# makezipfiles("data", "zip")

# cofiles =[
# 'Registered_Voters_List_Part2.txt',
# 'Registered_Voters_List_Part3.txt',
# 'Registered_Voters_List_Part4.txt',
# 'Registered_Voters_List_Part5.txt',
# 'Registered_Voters_List_Part6.txt',
# 'Registered_Voters_List_Part7.txt',
# 'Registered_Voters_List_Part8.txt',
# ]

# for i in cofiles:
#  fname = "20220629/" + str(i)
#  processacsvfile(fname,"nationaldob", "nationalname")


# current_directory = os.getcwd()
# directory_list = os.listdir()
# if 'nationaldob' not in directory_list:
#     os.mkdir(current_directory+'/nationaldob')
# if 'nationalname' not in directory_list:
#     os.mkdir(current_directory + '/nationalname')



   # file = "statewide/" + file
  processacsvfile(sampleNY.csv, "nationaldob", "nationalname")
   
# processacsvfile("sampleNY.txt", "nationaldob", "nationalname")
