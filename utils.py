#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Utility functions for Yamnnet/Cacophony Audio Classification Experiments


Created on Mon Aug 30 11:03:42 2021

@author: que
"""

import csv
import io
import os
import os.path
import glob
import datetime

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_io as tfio

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import patches
from IPython import display
import pydub
from scipy import signal
import pandas as pd

BIRDS_CLASSES = np.array([93,93,95,96,97,98,99,100,101,102,106,107,108,109,110,111,113,114,115])

def load_model_and_class_names():
    """
    Use tensorflow_hub to load the pre-trained model and a list of all the 521 class names:
    """
    # Load the model.
    model = hub.load("./yamnet_1/")

    # Find the name of the class with the top score when mean-aggregated across frames.
    def class_names_from_csv(class_map_csv_text):
        """Returns list of class names corresponding to score vector."""
        class_map_csv = io.StringIO(class_map_csv_text)
        class_names = [display_name for (class_index, mid, display_name) in csv.reader(class_map_csv)]
        class_names = class_names[1:]  # Skip CSV header
        return class_names
    class_map_path = model.class_map_path().numpy()
    class_names = class_names_from_csv(tf.io.read_file(class_map_path).numpy().decode('utf-8'))
    
    
    return (model,class_names)

def load_audio_16k_mono(filename, out_sample_rate=16000, target_db = -20,
                        start_time = 0, end_time=40, stream = None,
                        stream_type = "mp4"):
    """ Load an audio file in WAV or MP3/MP4 format based on the suffix,
    convert it to a float tensor, resample to 16 kHz single-channel audio. 

    Sound files will be standardized to have average amplitude of target_db

    Start and end times are in seconds

    if stream is not None then it's treated as a readable object (an open file
    or a BytesIO stream). In this case the type is specified by "stream_type"

    """

    if stream is not None:
        a = pydub.AudioSegment.from_file(stream, stream_type)
        
    else:
    
        try:
            suffix = os.path.splitext(filename)[1]
        except:
            "Unknown file suffix for {}".format(filename)
        suffix = suffix.lower()
        if suffix == '.wav':
            file_type = "wav"
        elif suffix == '.mp3':
            file_type = "mp3"
        elif suffix == '.mp4':
            file_type = "mp4"
        else:
            print("Unknown file type: {}".format(filename))
            raise ValueError

        a = pydub.AudioSegment.from_file(filename, file_type)


    # Resample to frame rate:
    a = a.set_frame_rate(out_sample_rate)
    # Normalize volume:
    gain_change = target_db - a.dBFS
    a = a.apply_gain(gain_change)

    wav = np.array(a.get_array_of_samples(), dtype='float32')
    wav = np.float32(wav) / 2**15
    if a.channels == 2:
        wav = wav.reshape((-1, 2))
    return wav[out_sample_rate*start_time:out_sample_rate*end_time]  



# High and Low Pass filters based on sci-py

def butter_highpass_filter(data, cutoff, fs, order=5):
    def butter_highpass(cutoff, fs, order=5):
        nyq = 0.5 * fs
        normal_cutoff = cutoff / nyq
        b, a = signal.butter(order, normal_cutoff, btype='high', analog=False)
        return b, a
    b, a = butter_highpass(cutoff, fs, order=order)
    y = signal.filtfilt(b, a, data)
    return y

def butter_lowpass_filter(data, cutoff, fs, order=5):
    def butter_lowpass(cutoff, fs, order=5):
        nyq = 0.5 * fs
        normal_cutoff = cutoff / nyq
        b, a = signal.butter(order, normal_cutoff, btype='low', analog=False)
        return b, a
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = signal.filtfilt(b, a, data)
    return y


def filename_to_date(filename):
    """
    Parse a datetime object from recording filename
    """
    base_name = os.path.splitext(os.path.basename(filename))[0]
    return datetime.datetime.strptime(base_name, '%Y%m%d-%H%M%S')


def read_cacophony_indices(directory):
    """
    Look for a file called "recordings.csv" in directory. If this file exists then read it using
    a CSV reader. Extract out the Cacophony indices and return them in a list in reversed order to 
    match the order of the recordings in the directory
    """
    file_name = "{}/recordings.csv".format(directory)
    cacophony_indices = []
    if os.path.exists(file_name):
        with open(file_name) as f:
            recordings_file = csv.DictReader(f)
            for row in recordings_file:
                ci = row["Cacophony Index"]
                cis = ci.split(";")
                # Average the first two values:
                cacophony_indices.append((float(cis[0])+float(cis[1]))/2.0)
    
    return cacophony_indices

