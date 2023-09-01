class Reducer:
    def __init__(self):
        self.result = {}


    def reduce(self, mapped_data):
        for line in mapped_data:
            # Dividir cada línea en clave y valor
            parts = line.split()
            key, value = parts
            if key in self.result:
                self.result[key] += int(value)  # Convierte el valor a entero
            else:
                self.result[key] = int(value)  # Convierte el valor a entero
        self.save_result_to_txt()

    def save_result_to_txt(self):
        with open("./results/reduce.txt", 'w', encoding='utf-8') as file:
            for key, value in self.result.items():
                file.write(f"{key}\t{value}\n")
