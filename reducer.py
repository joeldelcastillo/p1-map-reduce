import os


class Reducer:

    fileNumber: int

    def __init__(self, chunk_identifier, fileNumber, textFiles):
        self.chunk_identifier = chunk_identifier
        self.result = {}
        self.fileNumber = fileNumber
        self.textFiles = textFiles

    def reduce(self):
        for nameFile in self.textFiles:
            with open("./3_shuffled_words/%s" % nameFile, "rt") as file:

                for line in file:
                    parts = line.split()
                    key, value = parts

                    if key in self.result:
                        self.result[key] += int(value)
                    else:
                        self.result[key] = int(value)

        self.save_result_to_txt()

    def save_result_to_txt(self):

        output_directory = "./results"

        # Verify if path exists
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        with open(output_directory + "/reduced_%d.txt" % self.fileNumber, 'w', encoding='utf-8') as file:
            for key, value in self.result.items():
                file.write(f"{key}\t{value}\n")
            self.fileNumber += 1
