import os


class Map:
   
    def __init__(self, identifier, status_mappers):
        self.status_mappers = status_mappers
        self.identifier = identifier
        self.mapped_data = dict()

    def run(self):
        for i in range (len(self.status_mappers[self.identifier])):
            self.process_chunk(i)
            self.status_mappers[self.identifier][i] = True
            
        self.save_mapped_data_to_txt()

        # return(self.status_mappers[self.identifier])

    def clean_and_split(self, input_string):
        words = []
        current_word = []

        for char in input_string:
            if char.isalpha():
                current_word.append(char.lower())

            elif char.isspace():
                if (len(current_word) > 0):
                    words.append(''.join(current_word))
                    current_word = []

        if current_word:
            words.append(''.join(current_word))
            current_word = []

        return words

    def clean_split_and_count(self, input_string):
        current_word = []

        for char in input_string:
            if char.isalpha():
                current_word.append(char.lower())

            elif char.isspace():
                if (len(current_word) < 2):
                    current_word = []
                if (len(current_word) > 1):
                    word = ''.join(current_word)
                    if word not in self.mapped_data:
                        self.mapped_data[word] = 0
                    self.mapped_data[word] += 1
                    current_word = []

        if current_word:
            word = ''.join(current_word)
            if word not in self.mapped_data:
                self.mapped_data[word] = 0
            self.mapped_data[word] += 1
            current_word = []

        return self.mapped_data


    def process_chunk(self, current_chunk):
        chunk_number = (len(self.status_mappers[0]) * self.identifier) + current_chunk
        path = self.get_chunk_path(chunk_number)
        text_file = open(path, encoding="utf-8")
        data = text_file.read()
        words = self.clean_split_and_count(data)
        text_file.close()

    def get_chunk_path(self, current_chunk):
        path = "./1_fragments"
        chunk_path = f"{path}/chunk_{current_chunk}.txt"
        return chunk_path


    def save_mapped_data_to_txt(self):

        with open("./2_mapped_chunks/mapped_data_%d.txt" % self.identifier, 'w', encoding='utf-8') as file:
            for key, value in self.mapped_data.items():
                file.write(f"{key}\t{value}\n")
                # file.write(f"{item}\t{item[1]}\n")
