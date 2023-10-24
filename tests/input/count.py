import sys

input_file = sys.argv[1]
output_file = 'count_results.txt'
char_count = {}

with open(input_file, 'r') as file:
    for line in file:
        for char in line:
            char = char.lower()
            char_count[char] = char_count.get(char, 0) + 1

with open(output_file, 'w') as file:
    file.write(str(char_count))

