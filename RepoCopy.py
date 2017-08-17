import threading
import csv
import Queue
from shutil import copytree
import os
from copy import deepcopy
import time
import sys
import math

error_messages = {
    'big_file': "File is too large to copy using this tool ( > 3GB)",
    'size not equals': "target and source aren't the same size",
    'cannot overwrite': "Target directory already exists, cannot overwrite",
    'sys error': "System Error, failed to copy file",
    'source error': "Source path isn't a directory/doesn't exist"
}

report_messages = {
    'finished': "\n copied {} projects, in {:.2} minutes",
    'progress_with_fail': "copying{}    done with {} projects out of {}, {} projects failed - check report     \r",
    'progress': "copying{}    done with {} projects out of {}     \r",
}


def copier(paths_queue, reports_list, success_lock, fail_lock):
    global fail_counter, success_counter
    report_template = {'source': None, 'target': None, 'status': None, 'bytes copied': None, 'notes': None, }
    while True:
        paths = paths_queue.get()
        if paths != 'done':
            source = paths[0]
            target = paths[1]
            if os.path.isdir(source):
                try:
                    source_size = get_size(source)
                    global max_dir_size

                    if source_size > max_dir_size:
                        reports_list.append(
                            update_report(report_template, source, target,
                                          error_messages['big_file']))
                        with fail_lock:
                            fail_counter += 1
                        continue
                    copytree(source, target)
                    target_size = get_size(target)
                    # checks if size of src and dest is equals after copy
                    if target_size != source_size:
                        reports_list.append(
                            update_report(report_template, source, target,
                                          error_messages['size not equals'],
                                          convert_size(source_size), convert_size(target_size)))
                        with fail_lock:
                            fail_counter += 1
                        continue
                    else:
                        # case of success copy
                        reports_list.append(
                            update_report(report_template, source, target, source_size=convert_size(source_size),
                                          target_size=convert_size(target_size)))
                        with success_lock:
                            success_counter += 1
                except OSError:
                    reports_list.append(update_report(report_template, source, target,
                                                      error_messages['size not equals'], ))
                    with fail_lock:
                        fail_counter += 1
                except IOError as e:
                    reports_list.append(update_report(report_template, source, target,
                                                      error_messages['sys error'] + str(e), ))
                    with fail_lock:
                        fail_counter += 1
            else:

                reports_list.append(update_report(report_template, source, target,
                                                  error_messages['source error'], ))
                with fail_lock:
                    fail_counter += 1
        else:
            break


def parser(file, paths_queue):
    with open(file) as csv_file:
        lines = csv.DictReader(csv_file)
        for line in lines:
            if line:
                src = line['source']
                dest = line['target']
                paths_queue.put([src, dest])


def update_report(template, source, target, notes=None, source_size=None, target_size=None, ):
    report = deepcopy(template)
    report['source'] = source
    report['target'] = target
    if notes is not None:
        report['notes'] = notes
        report['status'] = 'failed'
    else:
        report['notes'] = ''
        report['status'] = 'copy complete'
    if source_size is not None and target_size is not None:
        report['bytes copied'] = str(source_size) + ' \\ ' + str(target_size)
    else:
        report['bytes copied'] = 0
    return report


def get_size(directory_path):
    """
    returns the size of a directory in bytes
    :param directory_path - path of the directory
    """

    folder_size = 0
    for (path, dirs, files) in os.walk(directory_path):
        for file in files:
            filename = os.path.join(path, file)
            folder_size += os.path.getsize(filename)
    return folder_size


def convert_size(size):
    if size == 0:
        return '0B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    return '%s %s' % (s, size_name[i])


if __name__ == '__main__':
    start_time = time.time()
    # queue to hold all copy paths for worker threads
    q = Queue.Queue()

    # thread to parse the csv paths input file
    reader = threading.Thread(target=parser, args=(sys.argv[1], q))
    reader.daemon = True
    reader.start()

    # list of copy results to export as report
    reports_lst = []

    # max directory file to copy 1GB
    max_dir_size = 3221225471

    # counts number of projects copied
    success_counter = 0
    fail_counter = 0

    # locks for counter thread control
    success_lock = threading.Lock()
    fail_lock = threading.Lock()

    reader.join()
    # number of repos to copy
    total = q.qsize()

    # start all working threads , each worker tries to enqueue a path or blocks
    workers = []

    # number of worker threads copying repositories
    num_of_workers = 64 if total > 64 else total

    for i in range(num_of_workers):
        q.put('done')

    # start workers
    for i in range(num_of_workers):
        worker = threading.Thread(target=copier, args=(q, reports_lst, success_lock, fail_lock,))
        workers.append(worker)
        worker.daemon = True
        worker.start()

    # console progress update
    while success_counter + fail_counter < total:
        for i in range(1, 4):
            dots = "." * i
            if fail_counter > 0:
                sys.stdout.write(
                    report_messages['progress_with_fail'].format(dots, success_counter, total, fail_counter))
                sys.stdout.flush()
                time.sleep(1)
            else:
                sys.stdout.write(
                    report_messages['progress'].format(dots, success_counter, total))
                sys.stdout.flush()
                time.sleep(1)

    # wait for workers to finish
    for worker in workers:
        worker.join()


    # create report file
    with open('report ' + str(time.time()) + '.csv', 'w') as csvfile:
        fieldnames = ['source', 'target', 'status', 'bytes copied', 'notes']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for line in reports_lst:
            writer.writerow(line)
    print(report_messages['finished'].format(success_counter, (time.time() - start_time) / 60))
