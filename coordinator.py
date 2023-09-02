from mapper import Map
from reducer import Reducer
from shuffler import Shuffler

import threading
import os

def partitions(file):

    file_size = os.path.getsize(input_file)
    chunk_size = file_size // (26 * 2) #letters in the anglophone alphabet
    # SIZE_HINT = 20*1024*1024
    fileNumber = 0 
    with open(file, "rt", encoding="utf-8") as f:
        while True:
            buf = f.read(SIZE_HINT)
            if not buf:
                break
            outFile = open("./fragments/chunk_%d.txt" % fileNumber, "wt", encoding="utf-8")
            outFile.write(buf)
            outFile.close()
            fileNumber += 1


if __name__ == "__main__":
    
    partitions("./input/encyclopedia.txt")
    map_instance = Map()
    map_instance.chunk_identifier = 0
    map_instance.run()

    # Create a lock to synchronize access to the file
    # file_lock = threading.Semaphore()

    file_locks = [threading.Semaphore(1) for _ in range(26)]

    # file_locks = {
    #     'a': threading.Lock(), 'b': threading.Lock(), 'c': threading.Lock(), 'd': threading.Lock(), 'e': threading.Lock(),
    #     'f': threading.Lock(), 'g': threading.Lock(), 'h': threading.Lock(), 'i': threading.Lock(), 'j': threading.Lock(),
    #     'k': threading.Lock(), 'l': threading.Lock(), 'm': threading.Lock(), 'n': threading.Lock(), 'o': threading.Lock(),
    #     'p': threading.Lock(), 'q': threading.Lock(), 'r': threading.Lock(), 's': threading.Lock(), 't': threading.Lock(),
    #     'u': threading.Lock(), 'v': threading.Lock(), 'w': threading.Lock(), 'x': threading.Lock(), 'y': threading.Lock(), 
    #     'z': threading.Lock()
    # }

    # reducer_instance = Reducer()
    # mapped_data = open("./results/mapped_data.txt", "rt", encoding="utf-8")
    # reducer_instance.reduce(mapped_data)
    # print("Done")







# input_string = "Hello, hello World! This is a test_string. \n It's example_123."
# result = clean_split_and_count(input_string)
# print(result)

