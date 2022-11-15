import os
from pathlib import Path

# home/workspaces/mark/Video_Data/kinetics-datasets-downloader

DATASET_ROOT = "dataset"
TRAIN_ROOT = os.path.join(DATASET_ROOT, "train/normal")
VALID_ROOT = os.path.join(DATASET_ROOT, "val/normal")
TEST_ROOT = os.path.join(DATASET_ROOT, "test/normal")

TRAIN_FRAMES_ROOT = os.path.join(DATASET_ROOT, "train_frames/normal")
VALID_FRAMES_ROOT = os.path.join(DATASET_ROOT, "val_frames/normal")
TEST_FRAMES_ROOT = os.path.join(DATASET_ROOT, "test_frames/normal")

TRAIN_SOUND_ROOT = os.path.join(DATASET_ROOT, "train_sound/normal")
VALID_SOUND_ROOT = os.path.join(DATASET_ROOT, "val_sound/normal")
TEST_SOUND_ROOT = os.path.join(DATASET_ROOT, "test_sound/normal")

CATEGORIES_PATH = "../resources/categories.json"
CLASSES_PATH = "../resources/classes.json"

TRAIN_METADATA_PATH = "../resources/700/train.json"
VAL_METADATA_PATH = "../resources/700/validate.json"
TEST_METADATA_PATH = "../resources/700/test.json"

SUB_CLASS_PATH = "../resources/700/categories.json"