from mrjob.job import MRJob
from mrjob.step import MRStep
import string


class WordFrequency(MRJob):
    uin = [word.strip().lower() for word in input("Enter word to check its frequency in document  : ").lower().split(',')]
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_words,
                   reducer=self.reducer_count_frequency)
        ]

    def mapper_extract_words(self, _, text):
        translator = str.maketrans('', '', string.punctuation)
        text_without_punctuations = text.translate(translator)
        

        words_list = text_without_punctuations.split()

        for word in words_list:
            yield word.lower(), 1

    def reducer_count_frequency(self, key, values):
        if key in self.uin:
            yield key, sum(values)


if __name__ == "__main__":
    WordFrequency.run()


"""
ren word_freq.py word_freq.py
python word_freq.py story.txt

"""