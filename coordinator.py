from mapper import Map
from reducer import Reducer
from shuffler import Shuffler

import threading
import os
import shutil
import numpy as np
import multiprocessing


class Coordinator:
    status_mappers: list
    status_shufflers: list
    status_reducers: list

    def __init__(self, input_file):
        self.cpu_count = os.cpu_count()
        self.input_file = input_file

    def partitions(self):

        file_size = os.path.getsize(self.input_file)
        # SIZE_HINT = file_size // (26 * 2) #letters in the anglophone alphabet
        SIZE_HINT = file_size // (10)  # letters in the anglophone alphabet
        # SIZE_HINT = 20*1024*1024
        char = ""
        fileNumber = 0

        output_directory = "./1_fragments"

        # Verify if path exists
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        with open(self.input_file, "rt", encoding="utf-8") as f:
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


    def merge_files(self):
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
    
    def run_mapper(self, mapper):
        return mapper.run()

    def run_reducer(self, reducer):
        return reducer.reduce()

    def run(self):
        num_to_alpha = {
            0: 'a', 1: 'b', 2: 'c', 3: 'd', 4: 'e', 5: 'f', 6: 'g', 7: 'h', 8: 'i', 9: 'j',
            10: 'k', 11: 'l', 12: 'm', 13: 'n', 14: 'o', 15: 'p', 16: 'q', 17: 'r', 18: 's',
            19: 't', 20: 'u', 21: 'v', 22: 'w', 23: 'x', 24: 'y', 25: 'z'
        }
        self.partitions()
        
        _, _, files = next(os.walk("./1_fragments"))
        mappers_count = len(files)
        shufflers_count = mappers_count
        reducers_count = mappers_count // 2

        ## create statuses per node





        
        # Create instances of Mappers

        map_pool = multiprocessing.Pool(processes=mappers_count)
        reducer_pool = multiprocessing.Pool(processes=reducers_count)
        mappers = [Map(i) for i in range(mappers_count)]
        map_pool.map(self.run_mapper, mappers)
        map_pool.close()
        map_pool.join()

        # Create instances of Shufflers
        file_locks = [threading.Semaphore(1) for _ in range(26)]
        # Create a lock to synchronize access to the files
        shufflers = [Shuffler(i, file_locks) for i in range(mappers_count)]
        shufflers_threads = []
        for shuffler in shufflers:
            thread = threading.Thread(target=shuffler.run)
            shufflers_threads.append(thread)

        # Starting Shuffler Threads
        for thread in shufflers_threads:
            thread.start()

        # Wait for all Shuffler Threads to finish
        for thread in shufflers_threads:
            thread.join()

        # Create instances of Reducers
        shuffler_list = os.listdir('./3_shuffled_words')
        shuffler_list = np.array_split(shuffler_list, reducers_count)
        print(shuffler_list)
        reducers = [Reducer(num_to_alpha[i], i, list(shuffler_list[i]))
                    for i in range(reducers_count)]

        reducer_pool.map(self.run_reducer, reducers)
        reducer_pool.close()
        reducer_pool.join()

        self.merge_files()



if __name__ == "__main__":

    coordinator = Coordinator("./0_input/encyclopedia.txt")
    coordinator.run()


















    
