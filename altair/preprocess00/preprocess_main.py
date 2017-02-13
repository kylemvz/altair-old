from multiprocessing import Process, Queue
from lib2to3.refactor import RefactoringTool, get_fixers_from_package
import queue as Q

import gzip
import json
import os

from altair.util.log import getLogger

logger = getLogger(__name__)

DONE_TOKEN = "DONE"

def iter_directory(dirpath):
    return [os.path.join(dirpath, f) for f in sorted(os.listdir(dirpath))]

def extract(path_to_json_gz):
    with gzip.open(path_to_json_gz, "rb") as f1:
        for line in f1:
            j = json.loads(line.decode("utf-8"))
            yield j

def convert_to_py3(py2_str):
    fixers = get_fixers_from_package('lib2to3.fixes')
    refactoring_tool = RefactoringTool(fixer_names=fixers)
    node3 = refactoring_tool.refactor_string(py2_str, 'script')
    py3_str = str(node3)
    return py3_str

def process_errors(errors, output_dir, fileno):
    output_file = os.path.join(output_dir, "errors%s.json.gz" % str(fileno).zfill(3))
    with gzip.open(output_file, "wb") as f:
        for error in errors:
            logger.info("Writing error: %s" % error["id"])
            f.write(json.dumps(error, ensure_ascii=False).encode("utf-8"))

def error_worker(q, output_dir, num_workers, lines_per_file):
    errors = []
    itr = 1
    received_done = 0
    while True:
        try:
            error_json = q.get()
            if error_json == DONE_TOKEN:
                received_done += 1
                if received_done >= num_workers:
                    logger.info("Received DONE. Flushing errors and closing worker.")
                    process_errors(errors, output_dir, itr)
                    break
                else:
                    logger.info("Received DONE (%s of %s)." % (received_done, num_workers))
                    continue
            errors.append(error_json)
            if len(errors) >= lines_per_file:
                logger.info("Writing errors to file...")
                process_errors(errors, output_dir, itr)
                errors = []
                itr += 1
        except Q.Empty:
            pass

def worker(q, output_dir, q2):
    while True:
        try:
            input_file = q.get(block=True, timeout=5)
            logger.info("Processing: %s" % input_file)
            output_file = os.path.join(output_dir, os.path.basename(input_file))
            with gzip.open(output_file, "wb") as f:
                for json_obj in extract(input_file):
                    logger.info("Converting: %s" % json_obj["id"])
                    try:
                        utf8 = (json_obj["content"].strip()+"\n").encode("utf-8").decode("utf-8")
                        py3_content = convert_to_py3(utf8)
                    except Exception as e:
                        logger.info("Error converting, skipping: %s" % json_obj["id"])
                        q2.put(json_obj)
                        continue
                    json_obj["content"] = py3_content
                    f.write(json.dumps(json_obj, ensure_ascii=False).encode("utf-8"))
        except Q.Empty:
            logger.info("Closing worker (empty queue).")
            q2.put(DONE_TOKEN)
            break

def main(args):
    workers = []
    q = Queue()
    for f in iter_directory(args.input_dir):
        q.put(f)

    q2 = Queue()

    for i in range(args.num_workers):
        p = Process(target=worker, args=(q, args.output_dir, q2))
        workers.append(p)
        p.start()

    p = Process(target=error_worker, args=(q2, args.output_dir, args.num_workers, args.error_file_length))
    p.start()

    q.close()
    for w in workers:
        w.join()
    q.join_thread()
    q2.close()
    p.join()
    q2.join_thread()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Preprocess .gz archive to extract json objects (newline-delimited) containing Python scripts, run 2to3 on script, then create a new json.gz archive.')

    # Required args
    parser.add_argument("input_dir",
                        type=str,
                        help="Input directory")
    parser.add_argument("output_dir",
                        type=str,
                        help="Output directory")

    # Optional args
    parser.add_argument("--num_workers",
                        type=int,
                        default=1,
                        help="Number to parallelize")
    parser.add_argument("--error_file_length",
                        type=int,
                        default=3000,
                        help="Max lines for error .json.gz files before rollover.")

    args = parser.parse_args()
    main(args)
