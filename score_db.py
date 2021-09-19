#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 07:46:09 2021


Script to score files in the Cacophony Database using Yamnet based scoring

Results are stored in an SQLite database



@author: que
"""

import sqlite3
import io
import utils
import score

import datetime
import argparse
from dateutil.parser import parse as parsedate
from dateutil.tz import tzlocal
from cacophonyapi.user import UserAPI as API
local_tz = tzlocal()

from private import private_data


DEFAULT_START_DATE = datetime.date(2021,9,17)
DEFAULT_END_DATE = datetime.datetime.today().date()
DEFAULT_DB_NAME = "scores.db"
DEFAULT_LOW_PASS = 4000
DEFAULT_HIGH_PASS = 2000
DEFAULT_NUM_OFFSETS = 3
DEFAULT_SCORE_THRESHOLD= 0.4
DEFAULT_TOP_K_CLASS = 3
SAMPLE_RATE = 16000


def parse_arguments():
    """
    
    Parse the command line arguments

    Returns
    -------
    Args 

    """

    parser = argparse.ArgumentParser()
    

    parser.add_argument(
        "--start-date",
        type=parsedate,
        default=DEFAULT_START_DATE,
        help="If specified, only files recorded on or after this date will be scored.",
    )
    parser.add_argument(
        "--end-date",
        default=DEFAULT_END_DATE,
        type=parsedate,
        help="If specified, only files recorded before or on this date will be scored.",
    )
    parser.add_argument(
        "-d",
        "--database",
        type=str,
        default = DEFAULT_DB_NAME,
        help="Save scores into this SQlite3 Database",
    )

    parser.add_argument(
        "--highpass",
        type=int,
        default = DEFAULT_HIGH_PASS,
        help="Cutoff value for high pass filtering")
    

    parser.add_argument(
        "--lowpass",
        type=int,
        default = DEFAULT_LOW_PASS,
        help="Cutoff value for low pass filtering" )
    

    parser.add_argument(
        "--num-offsets",
        type=int,
        default = DEFAULT_NUM_OFFSETS,
        help="Number of offsets in audio file during scoring" )
    

    parser.add_argument(
        "--score-threshold",
        type=int,
        default = DEFAULT_SCORE_THRESHOLD,
        help="Threshold for score to be counted as bird" )
    
    parser.add_argument(
        "--top-k-class",
        type=int,
        default = DEFAULT_TOP_K_CLASS,
        help="Top N for class score to be counted as bird" )
    
                                              

    args = parser.parse_args()
    
    
    
    
    return args


def create_sql_table(con):
    """
    
    Will create the SQL table "scores" if it doesn't already exist in the database with open connection con

    Parameters
    ----------
    con : SqLite3 Connection
        Connection to open database.

    Returns
    -------
    None

    """

    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS scores
                 (id INTEGER NOT NULL PRIMARY KEY,
                 ts TIMESTEMP,
                 dev_name TEXT,
                 dev_id INT,
                 thresh_score REAL,
                 class_score REAL,
                 ci_score REAL) ''')

    
    
def recording_in_db(rec_id, con):
    """
    
    Returns True if the recording with ID rec_id is already in the database
    pointed to by "con"
    
    Parameters
    ----------
    rec_id : int
        ID of th3 recording.
    con : SQLite3 connection
        Connection to open DB.

    Returns
    -------
    Bool - True if the rec_id is already an ID in the database, Fasle otherwise

    """
    
    cur = con.cursor()
    
    rows = cur.execute("SELECT id from scores where id = ?",(rec_id,))
    r = rows.fetchone()
    return r is not None
    
    
def extract_mean_ci_score(rec):
    """
    

    Parameters
    ----------
    rec : dict
        Audio recording record.

    Returns
    -------
    Mean cacaophony index as float or -1 if it could not be found.

    """
    
    ci = -1.0
    
    if "analysis" in rec["additionalMetadata"]:
        caco_id = rec['additionalMetadata']['analysis']['cacophony_index']
        if len(caco_id) > 1 :
            ci = caco_id[0]['index_percent'] + caco_id[1]['index_percent'] 
            ci = ci/200.0  # convert to fraction and calculate mean
    
            
    return ci

    

def insert_scores_into_db(con,scores, ci, rec):
    """
    

    Parameters
    ----------
    con : SQlite3 DB connection
        Connection to open sqlite DB.
    scores : tuple
        tuple of scores (class,thresh).
    ci : float
        cacophony score.
    rec : dict
        dictionary of audio record.

    Returns
    -------
    None.

    """
    rec_id = rec['id']
    dev = rec["Device"]
    dev_id = dev['id']
    dev_name = dev['devicename']
    dt = parsedate(rec["recordingDateTime"])
    dt = dt.astimezone(local_tz)
    
    cur = con.cursor()
    cur.execute("INSERT into scores VALUES ( ?,?,?,?,?,?,?)",( rec_id, dt,
                dev_name,dev_id, scores[1], scores[0], ci ))
        
     
    print("        Dev name: {:30s} Time = {}".format(dev_name, dt.strftime("%d-%h-%Y %H:%M:%S")))
    print("        Thresh = {:4.2f} Class = {:4.2f} CI = {:4.2f}".format(scores[1],scores[0],ci),flush=True  )                                                    
                                                                   
        
def score_recordings(recordings,args,con,model, client):
    """
    
    
    Score the recorings in the list "recordings"

    Parameters
    ----------
    recordings : list
        List of recording records.
    args : dict
        Dictionary of command-line parameter.
    con : TYPE
        connection to open SQLite database.
    model : Yamnet Model
        Yamnet model.
    client: API connection
    

    Returns
    -------
    None.

    """
    
    
    for rec in recordings:
        
        if recording_in_db(rec['id'], con):
            print ("      Recording {:6d} is already in the database".format(rec['id']))
        elif rec['processingState'] != 'FINISHED':
            print ("      Recording {:6d} has not been processed".format(rec['id']))
        else:
            print("       Downloading recording {:6d} ".format(rec['id']))                                                                        
              
            try:                                                           
                audio_data = client.download_raw(rec['id'])
                with io.BytesIO() as f:
                    for chunk in audio_data:
                        f.write(chunk)
                    f.seek(0,0)
                    scores =score.score_audio_file("",model, 
                                               utils.BIRDS_CLASSES,
                                               num_offsets=args.num_offsets, 
                                               low_pass_cutoff=args.lowpass,
                                               high_pass_cutoff=args.highpass, 
                                               score_threshold=args.score_threshold, 
                                               top_k_scores=args.top_k_class, 
                                               sample_rate=SAMPLE_RATE,
                                               stream=f,
                                               stream_type="mp4")     
                    ci = extract_mean_ci_score(rec)
                                                                 
                                                                         
                    insert_scores_into_db(con,scores, ci, rec) 
            except:
                print("       Failed downloading recording {:6d} ".format(rec['id'])) 
                
    
    



def score_all_recordings(args):
    """
    
    Download and score all the audio recordings between the specified dates args.start_date and
    args.end_date. This is done one day at a time to avoid limits on the maximum number of downloads
    

    Parameters
    ----------
    args : dictionary 
        Command line arguments.

    Returns
    -------
    None.

    """


    curr_date = args.start_date
    delta_date = datetime.timedelta(days=1)
    end_date = curr_date + delta_date
    
    model, class_names = utils.load_model_and_class_names()
    
    client = API(private_data['server'], private_data['username'],private_data['password'])
    

    con = sqlite3.connect(args.database, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    
    create_sql_table(con)

    while( curr_date < args.end_date):
        
        print("Working with date: ", curr_date.strftime("%d-%h-%Y"))
        
        recordings = client.query(limit=999999, type_='audio', startDate=curr_date,endDate=end_date)
        
        print("  There are {:4d} recordings".format(len(recordings)))
        
        
        score_recordings(recordings,args,con,model, client)
        
        con.commit()
        
        curr_date += delta_date
        end_date = curr_date + delta_date

    con.close()

def main():
    """
    
    Main routine - parses options, opens database, begins the scoring

    Returns
    -------
    None.

    """
    

    args =  parse_arguments()

    print("\nScoring data from server: {}".format(private_data["server"]))
    print("Using recordings from {} to {}".format(args.start_date.strftime("%d-%h-%Y"),
                                                   args.end_date.strftime("%d-%h-%Y") ))
    print("Scores stored in database: {}".format(args.database))

    print("\nStarted: ",datetime.datetime.today().strftime("%d-%h-%Y %H:%M:%S"))
   
    score_all_recordings(args)
 
    
    print("\nFinished: ",datetime.datetime.today().strftime("%d-%h-%Y %H:%M:%S"))
    
    
    

if __name__ == '__main__':
    main()