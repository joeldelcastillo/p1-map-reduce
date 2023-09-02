from mapper import Map
from reducer import Reducer
import threading

def partitions(file):

    SIZE_HINT = 20*1024*1024  # Size in bytes of the partition
    fileNumber = 0  # Number of the file
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

    # reducer_instance = Reducer()
    # mapped_data = open("./results/mapped_data.txt", "rt", encoding="utf-8")
    # reducer_instance.reduce(mapped_data)
    # print("Done")







# input_string = "Hello, hello World! This is a test_string. \n It's example_123."
# result = clean_split_and_count(input_string)
# print(result)

