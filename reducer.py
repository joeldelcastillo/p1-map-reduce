import os


class Reducer:

    identifier: int

    def __init__(self, identifier, textFiles, status_reducers):
        self.result = {}
        self.status_reducers = status_reducers
        self.identifier = identifier
        self.textFiles = textFiles

    def run(self):
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
        self.status_reducers[self.identifier] = True

    def save_result_to_txt(self):

        output_directory = "./4_reduced_words"

        # Verify if path exists
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        fileNumber = self.identifier

        with open(output_directory + "/reduced_%d.txt" % fileNumber, 'w', encoding='utf-8') as file:
            for key, value in self.result.items():
                file.write(f"{key}\t{value}\n")
            fileNumber += 1
