# leer mínimo un chunk
# flags

class Map:
    has_ended: bool
    failure: bool
    chuck_identifier: str
    max_retries: int

    def __init__(self):
        self.chuck_identifier = None
        self.failure = False
        self.has_ended = False

    def run(self):
        while True:
            if self.has_ended:
                break
            if self.failure:
                self.failure = False
            mapped_data = self.process_chunk()
            print(mapped_data)   #Debug

    def process_chunk(self):
        path = self.get_chunk_path()
        File = open(path)
        mapped_data = []
        for line in File:
            words = line.split()
            for word in words:
                mapped_data.append((word, 1))
        return mapped_data

    #def get_chunk_path(self):
        #Logica para obtener el path del chunk