
class Map:
    has_ended: bool
    failure: bool
    chunk_identifier: str
    max_failed_attempts: int

    def __init__(self):
        self.chunk_identifier = None
        self.failure = False
        self.has_ended = False
        self.max_failed_attempts = 3

    def run(self):
        while not self.has_ended:
            if self.failure:
                if self.max_failed_attempts <= 0:
                    self.has_ended = True
                else:
                    self.failure = False
            else:
                mapped_data = self.process_chunk()
                print(mapped_data)   #Debug

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
