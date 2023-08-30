# esto solo es dummy code

import threading
 
# Pame función para limpiar el texto


# Polito parte esto
def partitions(file):
    SIZE_HINT = 1
    # para pensar: qué pasa si le parte a una palabra?
    fileNumber = 0
    with open(file, "rt") as f:
        while True:
            buf = f.readlines(SIZE_HINT)
            print(buf)
            if not buf:
                # we've read the entire file in, so we're done.
                break
            outFile = open("./fragments/outFile%d.txt" % fileNumber, "wt")
            outFile.write(buf[0])
            outFile.close()
            fileNumber += 1
 
 
if __name__ =="__main__":

    partitions("./input.txt")
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