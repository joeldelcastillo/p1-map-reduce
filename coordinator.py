from mapper import Map
from reducer import Reducer
from shuffler import Shuffler

import threading
import os

def partitions(file):

    file_size = os.path.getsize(file)
    # SIZE_HINT = file_size // (26 * 2) #letters in the anglophone alphabet
    SIZE_HINT = file_size // (5) #letters in the anglophone alphabet
    # SIZE_HINT = 20*1024*1024
    fileNumber = 0 
    with open(file, "rt", encoding="utf-8") as f:
        while True:
            buf = f.read(SIZE_HINT)
            if not buf:
                break
            outFile = open("./1_fragments/chunk_%d.txt" % fileNumber, "wt", encoding="utf-8")
            outFile.write(buf)
            outFile.close()
            fileNumber += 1


if __name__ == "__main__":
    
    partitions("./0_input/encyclopedia.txt")

    _, _, files = next(os.walk("/usr/lib"))
    mappers_count = len(files)
    shufflers_count = mappers_count
    reducers_count = mappers_count // 2

    # Create instances of Mappers
    mappers = [Map(i) for i in range(mappers_count)]
    mappers_threads = []
    for mapper in mappers:
        thread = threading.Thread(target=mapper.run)
        mappers_threads.append(thread)

    # Create instances of Shufflers
    file_locks = [threading.Semaphore(1) for _ in range(26)]
    shufflers = [Shuffler(i, file_locks) for i in range(mappers_count)]
    shufflers_threads = []
    for shuffler in shufflers:
        thread = threading.Thread(target=shuffler.run)
        shufflers_threads.append(thread)

    # Create instances of Reducers


    # Create a lock to synchronize access to the file


        # Wait for all threads to finish
    for thread in threads:
        thread.join()


    # reducer_instance = Reducer()
    # mapped_data = open("./results/mapped_data.txt", "rt", encoding="utf-8")
    # reducer_instance.reduce(mapped_data)
    # print("Done")







# input_string = "Hello, hello World! This is a test_string. \n It's example_123."
# result = clean_split_and_count(input_string)
# print(result)

