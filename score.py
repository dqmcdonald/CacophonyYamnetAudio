#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Scoring functions for Yamnnet/Cacophony Audio Classification Experiments


Created on Mon September 13th

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
import utils


def score_audio_file(filename,
                      model,
                      bird_classes,
                      num_offsets = 5,
                      low_pass_cutoff = None,
                      high_pass_cutoff = None,
                      score_threshold = 0.4,
                      top_k_scores = 3,
                      sample_rate = 16000,
                      stream = None,
                      stream_type = "mp3",
                      verbose=False):
    """
    Read audio data from the wave or mp3 file given by "filename"

    If stream is not None then it is assumed to be a stream like object
    from which the audio data can be read in format "stream_type"

    Audio sample rate is expected to be "sample_rate".  

    If high_pass_cutoff and low_pass_cutoff are not None then lowpass and/or highpass filtering will
    be performed
    Run the sound data against the model and count the number of bird like 
    sounds. Run the model num_offsets time, offsetting the start of the file 
    by 1/num_offsets of a frame each time.

    If a bird like sound has a threshold greater than score_threshold that 
    counts towards the threshold score.

    For each frame one of the bird classes is in the top_k_scores, then 
    count that frame as containing a bird. 

    Scores are averaged over all offsets and the number of frames to return
    values from 0-1

    Return a tuple of the count score and the threshold score
    """

    wave_data = utils.load_audio_16k_mono(filename, 
        out_sample_rate=sample_rate,stream=stream, stream_type=stream_type)

    if low_pass_cutoff != None:
        wave_data =utils.butter_lowpass_filter( wave_data, low_pass_cutoff,
        sample_rate, order=5)

    if high_pass_cutoff != None:
        wave_data =utils.butter_highpass_filter( wave_data, high_pass_cutoff,
        sample_rate, order=5)


    sum_threshold_counts = 0
    sum_top_class_counts = 0
    offset =0

    for i in range(num_offsets):

        scores, embeddings, spectrogram = model(wave_data[i*offset:])

        top_scores = tf.math.top_k(
        scores, k=top_k_scores, sorted=True, name=None)

        top_scores = top_scores[1].numpy()
        scores_np = scores.numpy()



        if i == 0:
            # Set some parameters based on the first model applicatoin
            num_frames = len(scores_np)
            offset = int((len(wave_data)/num_frames)/num_offsets)


        scores_above_threshold  = scores_np[:,:] > score_threshold

        # Count the number of birds in each frame
        threshold_count = 0
        top_class_count = 0
        for frame in range(scores_np.shape[0]):
            if np.any(scores_above_threshold[frame,bird_classes]):
                    threshold_count += 1


            for frame_class in top_scores[frame,:]:
                if frame_class in bird_classes:
                    top_class_count += 1
                    break

        sum_threshold_counts += threshold_count

        sum_top_class_counts += top_class_count

        if verbose:
            print("{:2d} Offset = {:5.3f}, Offset thresh count = {:3d}".format(
                i,(i*offset)/SAMPLE_RATE,threshold_count))
            print("{:2d} Offset = {:5.3f}, Offset top count = {:3d}".format(
                i,(i*offset)/SAMPLE_RATE,top_class_count))


    # Score is averaged over all the offset applications of the model and 
    # then divided by the number of frames to give a value in the range 0-1

    threshold_score = (sum_threshold_counts/float(num_offsets))/float(
            num_frames)

    top_class_score = (sum_top_class_counts/float(num_offsets))/float(
            num_frames)

    return( top_class_score, threshold_score )
