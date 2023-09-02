
class Map:
    has_ended: bool
    failure: bool
    chunk_identifier: str
    max_failed_attempts: int
    fileNumber: int

    def __init__(self):
        self.chunk_identifier = None
        self.failure = False
        self.has_ended = False
        self.max_failed_attempts = 3
        self.fileNumber = 0

    def run(self):
        while not self.has_ended:
            if self.failure:
                if self.max_failed_attempts <= 0:
                    self.has_ended = True
                else:
                    self.failure = False
            else:
                mapped_data = self.process_chunk()
                #print(mapped_data)   #Debug
                self.save_mapped_data_to_txt(mapped_data)

    def process_chunk(self):
        path = self.get_chunk_path()
        with open(path , encoding="utf-8") as File:
            mapped_data = []
            for line in File:
                words = line.split()
                for word in words:
                    mapped_data.append((word, 1))
        self.has_ended = True
        return mapped_data

    def get_chunk_path(self):
        path = "./fragments"  
        chunk_number = self.chunk_identifier
        chunk_path = f"{path}/chunk_{chunk_number}.txt"  
        return chunk_path

    def save_mapped_data_to_txt(self, mapped_data):
        with open("./results/mapped_data_%d.txt" % self.fileNumber, 'w', encoding='utf-8') as file:
            for item in mapped_data:
                file.write(f"{item[0]}\t{item[1]}\n")
            self.fileNumber += 1