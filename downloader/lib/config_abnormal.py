import os
from pathlib import Path

# home/workspaces/mark/Video_Data/kinetics-datasets-downloader

DATASET_ROOT = "dataset"
TRAIN_ROOT = os.path.join(DATASET_ROOT, "train/abnormal")
VALID_ROOT = os.path.join(DATASET_ROOT, "val/abnormal")
TEST_ROOT = os.path.join(DATASET_ROOT, "test/abnormal")

TRAIN_FRAMES_ROOT = os.path.join(DATASET_ROOT, "train_frames/abnormal")
VALID_FRAMES_ROOT = os.path.join(DATASET_ROOT, "val_frames/abnormal")
TEST_FRAMES_ROOT = os.path.join(DATASET_ROOT, "test_frames/abnormal")

TRAIN_SOUND_ROOT = os.path.join(DATASET_ROOT, "train_sound/abnormal")
VALID_SOUND_ROOT = os.path.join(DATASET_ROOT, "val_sound/abnormal")
TEST_SOUND_ROOT = os.path.join(DATASET_ROOT, "test_sound/abnormal")

CATEGORIES_PATH = "../resources/categories.json"
CLASSES_PATH = "../resources/classes.json"

TRAIN_METADATA_PATH = "../resources/700/train/kinetics_700_train.json"
VAL_METADATA_PATH = "../resources/700/val/kinetics_700_validate.json"
TEST_METADATA_PATH = "../resources/700/test/kinetics_700_test.json"

SUB_CLASS_PATH = "../resources/700/categories.json"