from mapper import Map
from reducer import Reducer
from shuffler import Shuffler

import threading
import os
import shutil


def partitions(file):

    file_size = os.path.getsize(file)
    # SIZE_HINT = file_size // (26 * 2) #letters in the anglophone alphabet
    SIZE_HINT = file_size // (5)  # letters in the anglophone alphabet
    # SIZE_HINT = 20*1024*1024
    char = ""
    fileNumber = 0

    output_directory = "./1_fragments"

    # Verify if path exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    with open(file, "rt", encoding="utf-8") as f:
        while True:
            buf = f.read(1)
            if not buf:
                break
            char += buf

            if buf.isspace() and len(char) >= SIZE_HINT:

                outFile = open(output_directory + "/chunk_%d.txt" %
                               fileNumber, "wt", encoding="utf-8")
                outFile.write(char)
                outFile.close()
                char = ""
                fileNumber += 1


def merge_files():
    path = "./results"
    output_file = "count_text.txt"

    files = [os.path.join(path, file)
             for file in os.listdir(path) if file.endswith('.txt')]

    with open(output_file, 'wt') as outfile:
        for file in files:
            with open(file, 'rt') as inputfile:
                shutil.copyfileobj(inputfile, outfile)

    with open(output_file, 'rt') as file:
        sort_text = sorted(file)

    with open(output_file, 'wt') as file:
        file.writelines(sort_text)


if __name__ == "__main__":
    num_to_alpha = {
        0: 'a', 1: 'b', 2: 'c', 3: 'd', 4: 'e', 5: 'f', 6: 'g', 7: 'h', 8: 'i', 9: 'j',
        10: 'k', 11: 'l', 12: 'm', 13: 'n', 14: 'o', 15: 'p', 16: 'q', 17: 'r', 18: 's',
        19: 't', 20: 'u', 21: 'v', 22: 'w', 23: 'x', 24: 'y', 25: 'z'
    }

    partitions("./0_input/encyclopedia.txt")

    _, _, files = next(os.walk("./1_fragments"))
    mappers_count = len(files)
    shufflers_count = mappers_count
    #reducers_count = mappers_count // 2
    reducers_count = 26
    # Create instances of Mappers

    # TO DO: Mappers should not take in count first and last word (because of fragmentation)
    mappers = [Map(i) for i in range(mappers_count)]
    mappers_threads = []
    for mapper in mappers:
        thread = threading.Thread(target=mapper.run)
        mappers_threads.append(thread)

    # Create instances of Shufflers
    file_locks = [threading.Semaphore(1) for _ in range(26)]
    # Create a lock to synchronize access to the files
    shufflers = [Shuffler(i, file_locks) for i in range(mappers_count)]
    shufflers_threads = []
    for shuffler in shufflers:
        thread = threading.Thread(target=shuffler.run)
        shufflers_threads.append(thread)

    # Create instances of Reducers
    reducers = [Reducer(num_to_alpha[i], i) for i in range(reducers_count)]
    reducers_threads = []

    for reducer in reducers:
        thread = threading.Thread(target=reducer.reduce)
        reducers_threads.append(thread)

    # Starting Mappers Threads
    for thread in mappers_threads:
        thread.start()

    # Wait for all Mapper Threads to finish
    for thread in mappers_threads:
        thread.join()

    # Starting Shuffler Threads
    for thread in shufflers_threads:
        thread.start()

    # Wait for all Shuffler Threads to finish
    for thread in shufflers_threads:
        thread.join()

    # Starting Reducer Threads
    for thread in reducers_threads:
        thread.start()

    # Wait for all Reducer Threads to finish
    for thread in reducers_threads:
        thread.join()

    merge_files()
