import os


class Map:
    has_ended: bool
    failure: bool
    chunk_identifier: str
    max_failed_attempts: int
    fileNumber: int

    def __init__(self, chunk_identifier):
        self.chunk_identifier = chunk_identifier
        self.failure = False
        self.has_ended = False
        self.max_failed_attempts = 3
        self.fileNumber = 0

    def run(self):
        mapped_data = self.process_chunk()
        self.save_mapped_data_to_txt(mapped_data)

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
        word_counter = {}
        current_word = []

        for char in input_string:
            if char.isalpha():
                current_word.append(char.lower())

            elif char.isspace():
                if (len(current_word) < 2):
                    current_word = []
                if (len(current_word) > 1):
                    word = ''.join(current_word)
                    if word not in word_counter:
                        word_counter[word] = 0
                    word_counter[word] += 1
                    current_word = []

        if current_word:
            word = ''.join(current_word)
            if word not in word_counter:
                word_counter[word] = 0
            word_counter[word] += 1
            current_word = []

        return word_counter


    def process_chunk(self):
        path = self.get_chunk_path()
        text_file = open(path, encoding="utf-8")
        data = text_file.read()
        words = self.clean_split_and_count(data)
        text_file.close()
        return words

    def get_chunk_path(self):
        path = "./1_fragments"
        chunk_number = self.chunk_identifier
        chunk_path = f"{path}/chunk_{chunk_number}.txt"
        return chunk_path

    def save_mapped_data_to_txt(self, mapped_data):

        with open("./2_mapped_chunks/mapped_data_%d.txt" % self.chunk_identifier, 'w', encoding='utf-8') as file:
            for key, value in mapped_data.items():
                file.write(f"{key}\t{value}\n")
                # file.write(f"{item}\t{item[1]}\n")
