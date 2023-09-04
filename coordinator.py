from mapper import Map
from reducer import Reducer
from shuffler import Shuffler

import threading
import os
import shutil
import numpy as np
import multiprocessing


import glob

class Coordinator:
    status_mappers: list
    status_shufflers: list
    status_reducers: list

    def __init__(self, input_file):
        self.cpu_count = os.cpu_count() if os.cpu_count() else 2
        self.input_file = input_file

    def partitions(self):

        SIZE_HINT = 20*1024*1024
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
        path = "./4_reduced_words"
        output_file = "RESULT.txt"

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

    def mapper_status_helper(self):
        helper_list = []
        for i in range(len(self.status_mappers)):
            current_failed_index = i
            for j in range(len(self.status_mappers[i])):
                if(self.status_mappers[i][j] == False):
                    helper_list.append(current_failed_index)
                    continue
        return helper_list


    def run_mappers(self):
        _, _, files = next(os.walk("./1_fragments"))
        statuses = ([False for _ in range(len(files))])
        statuses = np.array_split(statuses, self.cpu_count)
        self.status_mappers = [arr.tolist() for arr in statuses]
        mappers_count = self.cpu_count

        attempts = 0
        while attempts < 3:

            if(len(self.mapper_status_helper()) == 0):
                break

            mappers = [Map(self.mapper_status_helper()[i], self.status_mappers) for i in range(len(self.mapper_status_helper()))]
            mappers_threads = []
            for mapper in mappers:
                thread = threading.Thread(target=mapper.run)
                mappers_threads.append(thread)

            # Starting Mappers Threads
            for thread in mappers_threads:
                thread.start()

            # Wait for all Mapper Threads to finish
            for thread in mappers_threads:
                thread.join()
            
            attempts += 1

    def check_status_shufflers():
        for status in self.status_shufflers:
            if (status == False):
                return False
        return True

    def run_shufflers(self):

        attempts = 0
        while attempts < 3:
            
            if(self.check_status_shufflers == True):
                break

            directory = './3_shuffled_words'
            for _file in os.listdir(directory):
                file_route = os.path.join(directory, _file)
                if os.path.exists(file_route):
                    os.remove(file_route)
            
            shufflers_count = self.cpu_count
            self.status_shufflers = [False for _ in range(shufflers_count)]
            # Create instances of Shufflers
            file_locks = [threading.Semaphore(1) for _ in range(26)]
            # Create a lock to synchronize access to the files
            shufflers = [Shuffler(i, file_locks, self.status_shufflers) for i in range(shufflers_count)]
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

            attempts += 1

    def check_status_reducers(self):
        helper_list_reducer = []
        for index in range(len(self.status_reducers)):
            if (self.status_reducers[index] == False):
                helper_list_reducer.append(index)
        return helper_list_reducer
    
    def run_reducers(self):
        reducers_count = self.cpu_count // 2
        self.status_reducers = [False for _ in range(reducers_count)]

        
        shuffler_list = os.listdir('./3_shuffled_words')
        shuffler_list = np.array_split(shuffler_list, reducers_count)
        
        attempts = 0
        while attempts < 3:

            reducers_list = self.check_status_reducers()
            if(len(reducers_list) == 0):
                break
                
            reducers = [Reducer(i, list(shuffler_list[i]), self.status_reducers) for i in range(len(reducers_list))]
            reducer_threads = []
            for reducer in reducers:
                thread = threading.Thread(target=reducer.run)
                reducer_threads.append(thread)

            # Starting Shuffler Threads
            for thread in reducer_threads:
                thread.start()

            # Wait for all Shuffler Threads to finish
            for thread in reducer_threads:
                thread.join()

            print(self.status_reducers)
            attempts += 1

    def run(self):

        self.partitions()
        self.run_mappers()
        self.run_shufflers()
        self.run_reducers()
        self.merge_files()


if __name__ == "__main__":

    coordinator = Coordinator("./0_input/encyclopedia.txt")
    coordinator.run()


















    
