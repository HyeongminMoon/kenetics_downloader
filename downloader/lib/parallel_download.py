import os
import sys ####
import csv
from multiprocessing import Process, Queue, current_process
from pathlib import Path
import time

import lib.downloader as downloader

csv.field_size_limit(131072 * 100)

class Pool:
    """
    A pool of video downloaders.
    """

    def __init__(self, classes, videos_dict, directory, num_workers, failed_save_file, compress, verbose, skip, log_file=None, stats_file=None):
        """
        :param classes:               List of classes to download.
        :param videos_dict:           Dictionary of all videos.
        :param directory:             Where to download to videos.
        :param num_workers:           How many videos to download in parallel.
        :param failed_save_file:      Where to save the failed videos ids.
        :param compress:              Whether to compress the videos using gzip.
        """

        self.classes = classes
        self.videos_dict = videos_dict
        self.directory = directory
        self.num_workers = num_workers
        self.failed_save_file = failed_save_file
        self.compress = compress
        self.verbose = verbose
        self.skip = skip
        self.log_file = log_file
        self.stats_file = stats_file

        self.videos_queue = Queue(100)
        self.failed_queue = Queue(100)
        self.stats_queue = Queue(100)

        self.workers = []
        self.failed_save_worker = None
        self.stats_worker = None
        
        ####
        self.timeout = 500

        if verbose:
            print("downloading:")
            if self.classes is not None:
                for cls in self.classes:
                    print(cls)
                print()

    def feed_videos(self):
        """
        Feed video ids into the download queue.
        :return:    None.
        """
        print('feed_videos')
        print(self.classes)
        if self.classes is None:
            downloader.download_class_parallel(None, self.videos_dict, self.directory, self.videos_queue)
        else:
            for class_name in self.classes:
                if self.verbose:
                    print(class_name)
                class_path = os.path.join(self.directory, class_name.replace(" ", "_"))
                videos = list(self.videos_dict.keys())
                video_dict = dict()
                for video in videos:
                    if self.videos_dict.get(video).get('annotations').get('label') == class_name:
                        video_dict[video] = self.videos_dict.get(video)
                if not self.skip or not os.path.isdir(class_path):
                    downloader.download_class_parallel(class_name, video_dict, self.directory, self.videos_queue)
                print("after download_class_parallel, pid:", os.getpid())
                
                
            if self.verbose:
                print("done")

    def start_workers(self):
        """
        Start all workers.
        :return:    None.
        """
        # print("start_workers")
        # start failed videos saver
        if self.failed_save_file is not None:
            self.failed_save_worker = Process(target=write_failed_worker, args=(self.failed_queue, self.failed_save_file))
            self.failed_save_worker.start()

        # start stats worker
        if self.stats_file is not None:
            self.stats_worker = Process(target=write_stats_worker, args=(self.stats_queue, self.stats_file))
            self.stats_worker.start()

#         # start download workers
#         for _ in range(self.num_workers):
#             worker = Process(
#                 target=video_worker,
#                 args=(
#                     self.videos_queue, self.failed_queue, self.compress,
#                     self.log_file, self.failed_save_file, self.stats_queue)
#                 )
            
#             worker.start()
            
#             start = time.time()
#             self.workers.append(worker)
        ####
        print("watcher starting")
        watcher = Process(
            target=worker_watcher,
            args=(
                self.num_workers, self.timeout,
                self.videos_queue, self.failed_queue, self.compress,
                self.log_file, self.failed_save_file, self.stats_queue)
            )

        watcher.start()
        print("watcher started")
                
    def stop_workers(self):
        """
        Stop all workers.
        :return:    None.
        """
        print('stop workers')
        # send end signal to all download workers
        for _ in range(len(self.workers)):
            self.videos_queue.put(None)

        # wait for the processes to finish
        for worker in self.workers:
            worker.join()

        # end failed videos saver
        if self.failed_save_worker is not None:
            self.failed_queue.put(None)
            self.failed_save_worker.join()

        # End stats saver
        if self.stats_worker is not None:
            self.stats_queue.put(None)
            self.stats_worker.join()

def worker_watcher(num_workers, timeout, videos_queue, failed_queue, compress, log_file, failed_save_file, stats_queue):
    ####
    workers = []
    for _ in range(num_workers):
        worker = Process(
            target=video_worker,
            args=(
                videos_queue, failed_queue, compress,
                log_file, failed_save_file, stats_queue)
            )

        worker.start()
        workers.append(worker)
    
    ####
    start = time.time()
    reset_cnt = 0
    while(1):
        reset_cnt += 1
        time.sleep(10)
        print("(watcher)watcher started working")
        if reset_cnt % 60 == 0:
            print("(watcher)reset all workers...")
            new_workers = list()
            for r_worker in workers:
                # remove timed out worker
                print("(watcher)timed out, killing worker pid:", r_worker.pid)
                r_worker.terminate()
                r_worker.join()

                # add new worker
                n_worker = Process(
                    target=video_worker,
                    args=(
                        videos_queue, failed_queue, compress,
                        log_file, failed_save_file, stats_queue)
                    )
                check = True
                print("(watcher)creating new worker..")
                n_worker.start()
                new_workers.append(n_worker)
            workers = new_workers
        else:
            check = False
            for r_worker in workers:
                if not r_worker.is_alive():
                    # remove timed out worker
                    print("(watcher)timed out, killing worker pid:", r_worker.pid)
                    r_worker.terminate()
                    r_worker.join()
                    workers.remove(r_worker)

                    # add new worker
                    n_worker = Process(
                        target=video_worker,
                        args=(
                            videos_queue, failed_queue, compress,
                            log_file, failed_save_file, stats_queue)
                        )
                    check = True
                    print("(watcher)creating new worker..")
                    n_worker.start()
                    workers.append(n_worker)
            if not check:
                print("(watcher)nobody died.")
            # print(f"(watcher)workers:", {[worker.pid for worker in workers]})

            
def video_worker(videos_queue, failed_queue, compress, log_file, failed_log_file, stats_queue): 
    """
    Downloads videos pass in the videos queue.
    :param videos_queue:      Queue for metadata of videos to be download.
    :param failed_queue:      Queue of failed video ids.
    :param compress:          Whether to compress the videos using gzip.
    :param log_file:          Path to a log file for youtube-dl.
    :return:                  None.
    """
    ####
    print('(worker)start workers pid:', os.getpid())
    failed_ids = []

    lf_path = Path(failed_log_file)

    if lf_path.exists():
        with lf_path.open(mode='r') as lf:
            csv_reader = csv.reader(lf, delimiter=',')
            for row in csv_reader:
                failed_ids.append(row[0])

    # keep_going = True
    elapsed = 0
    i = 0
    s = 0
    init_time = time.time()
    life_time = 600
    # while time.time() - init_time <= life_time:
    for _ in range(1):
        try:
            time.sleep(.1)
            # print(f"getting... pid: ", os.getpid())
            request = videos_queue.get(timeout=120) # Timeout after 2 minutes
            ####
            print(f"get:{request} pid:", os.getpid())
            if request is None:
                print("request is None")
                stats_queue.put(None)
                break

            video_id, directory, start, end = request
            s += 1
            # print("1")
            if video_id in failed_ids:
                print('Skipping {} as previously failed'.format(video_id))
                continue
            # print("2")
            slice_path = "{}.mp4".format(os.path.join(directory, video_id))
            if os.path.isfile(slice_path):
                print('Exists skipping {}'.format(video_id))
                continue
            # print("3")
            start_time = time.time()
            result = downloader.process_video(video_id, directory, start, end, compress=compress, log_file=log_file)
            duration = round(time.time() - start_time, 1)
            elapsed += duration
            # print("4")
            current = current_process()
            label = Path(directory).stem
            # print("5")

            print("Completed {video_id}: duration={duration}s, download={download_duration}s, ffmpeg={ffmpeg_duration}s, elapsed={elapsed}s, avg={avg}s, i={i}, i+s={s}, id={cp}, pid={pid}, label={label}".format(
                video_id=video_id,
                duration=duration,
                download_duration=result.get("download_duration"),
                ffmpeg_duration=result.get("ffmpeg_duration"),
                elapsed=round(elapsed, 1),
                avg=round(elapsed / (i + 1), 1),
                i=i,
                s=s,
                cp=current.name,
                pid=current.pid,
                label=label)
              )

            result.update({
                'total_duration': duration,
                'elapsed': round(elapsed, 1),
                'average_duration': round(elapsed / (i + 1), 1),
                'iteration': i,
                'skipped_iteration': s,
                'queue_id': current.name,
                'pid': current.pid,
                'label': label
            })

            if result.get('success'):
                stats_queue.put(result)

            i += 1
            if not result.get('success'):
                if result.get('error') and 'HTTP Error 429' in result.get('error'):
                    print('Exceeded API Limit, no point in continuing')
                    break

            failed_queue.put(result)

        except Exception as e:
            print("Exception pid:", os.getpid(), "error:", str(e))
            failed_queue.put({ 'video_id': video_id, 'error': str(e) })
            break
    # else:
    #     print("else")
    print("(worker)life out. pid:", os.getpid())
    sys.exit(1)
            
def write_failed_worker(failed_queue, failed_save_file):
    """
    Write failed video ids into a file.
    :param failed_queue:        Queue of failed video ids.
    :param failed_save_file:    Where to save the videos.
    :return:                    None.
    """

    while True:
        error = failed_queue.get()
        if error is None:
            break

    with open(failed_save_file, "a") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([error.get('video_id'), error.get('label'), error.get('error')])


def write_stats_worker(stats_queue, stats_file):
    """
    Write failed video ids into a file.
    :param failed_queue:        Queue of failed video ids.
    :param failed_save_file:    Where to save the videos.
    :return:                    None.
    """

    fieldnames = [
    "video_id", "label", "status", "download_duration",
    "ffmpeg_duration", "total_duration", "average_duration", "elapsed",
    "iteration", "skipped_iteration", "queue_id", "pid"
    ]
    with open(stats_file, "a") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames
        )
    writer.writeheader()

    writer = csv.writer(csv_file)

    while True:
        stats = stats_queue.get()
        if not stats:
            break
        writer.writerow([stats[f] for f in fieldnames])