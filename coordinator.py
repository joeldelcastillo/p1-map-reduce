from mapper import Map
from reducer import Reducer
import threading

def clean(buf):
    #Logica de clean text
    pass


def partitions(file):

    SIZE_HINT = 20*1024*1024  # Size in bytes of the partition
    fileNumber = 0  # Number of the file
    # Open the file in read mode
    with open(file, "rt", encoding="utf-8") as f:
        while True:
            buf = f.readlines(SIZE_HINT)
            #print(buf)
            # use the function clean(buf)

            #buf = clean(buf)

            buf = "".join(buf)  # convert list to string
            #print("buff:", buf)
            if not buf:
                # we've read the entire file in, so we're done.
                break
            # create output file and write the partition

            outFile = open("./fragments/chunk_%d.txt" % fileNumber, "wt", encoding="utf-8")
            outFile.write(buf)
            outFile.close()
            fileNumber += 1


if __name__ == "__main__":
    
    partitions("./input/encyclopedia.txt")
    map_instance = Map()
    map_instance.chunk_identifier = 0
    map_instance.run()

    reducer_instance = Reducer()
    mapped_data = open("./results/mapped_data.txt", "rt", encoding="utf-8")
    reducer_instance.reduce(mapped_data)
    print("Done")