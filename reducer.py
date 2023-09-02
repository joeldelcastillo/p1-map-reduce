import os


class Reducer:

    fileNumber: int

    def __init__(self, chunk_identifier, fileNumber):
        self.chunk_identifier = chunk_identifier
        self.result = {}
        self.fileNumber = fileNumber

    def reduce(self):

        with open("./3_shuffled_words/%s_shuffled_bucket.txt" % self.chunk_identifier, "rt") as file:

            for line in file:
                print(line)
                # Dividir cada línea en clave y valor
                parts = line.split()
                print(parts)
                key, value = parts
                print(key, value)
                if key in self.result:
                    # Convierte el valor a entero
                    self.result[key] += int(value)
                else:
                    # Convierte el valor a entero`
                    self.result[key] = int(value)

            print(self.result)

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
