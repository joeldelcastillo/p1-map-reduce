
import threading

def partitions(file):

    SIZE_HINT = 20*1024*1024  # Size in bytes of the partition
    fileNumber = 0  # Number of the file
    # Open the file in read mode
    with open(file, "rt", encoding="utf-8") as f:
        while True:
            buf = f.readlines(SIZE_HINT)
            print(buf)
            # use the function clean(buf)

            # buf = clean(buf)--return an array

            buf = "".join(buf)  # convert list to string
            print("buff:", buf)
            if not buf:
                # we've read the entire file in, so we're done.
                break
            # create output file and write the partition

            outFile = open("./fragments/chunk_%d.txt" % fileNumber, "wt", encoding="utf-8")
            outFile.write(buf)
            outFile.close()
            fileNumber += 1


if __name__ == "__main__":

    partitions("./input.txt")
    #partitions("./large_encyclopedia2.txt")
    # creating thread
    # t1 = threading.Thread(target=print_square, args=(10,))
    # t2 = threading.Thread(target=print_cube, args=(10,))

    # # starting thread 1
    # t1.start()
    # # starting thread 2
    # t2.start()

    # # wait until thread 1 is completely executed
    # t1.join()
    # # wait until thread 2 is completely executed
    # t2.join()

    # # both threads completely executed
    # print("Done!")
