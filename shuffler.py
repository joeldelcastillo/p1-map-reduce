class Shuffler:

    def __init__(self, identifier, file_locks, status_shufflers):
        self.status_shufflers = status_shufflers
        self.result = [[] for _ in range(26)]
        self.identifier = identifier
        self.file = self.get_chunk_path()
        self.file_locks = file_locks
        self.completed_buckets = [False for _ in range(26)]
        self.alpha_to_num = {
            'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 5, 'g': 6, 'h': 7, 'i': 8, 'j': 9,
            'k': 10, 'l': 11, 'm': 12, 'n': 13, 'o': 14, 'p': 15, 'q': 16, 'r': 17, 's': 18,
            't': 19, 'u': 20, 'v': 21, 'w': 22, 'x': 23, 'y': 24, 'z': 25
        }
        self.num_to_alpha = {
            0: 'a', 1: 'b', 2: 'c', 3: 'd', 4: 'e', 5: 'f', 6: 'g', 7: 'h', 8: 'i', 9: 'j',
            10: 'k', 11: 'l', 12: 'm', 13: 'n', 14: 'o', 15: 'p', 16: 'q', 17: 'r', 18: 's',
            19: 't', 20: 'u', 21: 'v', 22: 'w', 23: 'x', 24: 'y', 25: 'z'
        }

    def run(self):
        self.shuffle_by_letter()
        self.test_and_set()
        self.status_shufflers[self.identifier] = True

    def get_chunk_path(self):
        path = "./2_mapped_chunks"
        chunk_number = self.identifier
        chunk_path = f"{path}/mapped_data_{chunk_number}.txt"
        return chunk_path

    def shuffle_by_letter(self):
        with open(self.file, "r") as file:
            for line in file:
                if line:
                    first_letter = line[0]
                    # if first_letter in letter_buckets:
                    self.result[self.alpha_to_num[first_letter]].append(line)
        
    def are_buckets_complete(self):
        for item in self.completed_buckets:
            if (item == False):
                return False
        return True

    def test_and_set(self):
        target = 0
        while (self.are_buckets_complete() == False):
            acquired = False
            if (self.completed_buckets[target] == False):
                acquired = self.file_locks[target].acquire(blocking=False)
            while (acquired == False):
                target = (target + 1) % 26
                acquired = self.file_locks[target].acquire(blocking=False)
            if acquired:
                try:  # Critical section
                    self.save_result_to_txt(self.result[target], target)
                    self.completed_buckets[target] = True
                    if (self.completed_buckets[(target + 1) % 26] == False):
                        target = (target + 1) % 26
                finally:
                    # Release the lock when done
                    self.file_locks[target].release()

    def save_result_to_txt(self, bucket, target):
        with open("./3_shuffled_words/%s_shuffled_bucket.txt" % self.num_to_alpha[target], 'a') as file:
            for item in self.result[target]:
                file.write(item)
