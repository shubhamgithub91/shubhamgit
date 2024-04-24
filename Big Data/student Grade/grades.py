from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class CalculateGrades(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        if line.startswith('id'):
            return

        reader = csv.reader([line])
        for row in reader:
            name, math, english = str(row[1]), int(row[2]), int(row[3])
            total_marks = math + english
            percentage = (total_marks / 200) * 100
            yield None, {'name': name, 'percentage': percentage}

    def reducer(self, key, values):
        for student_info in values:
            percentage = student_info['percentage']
            grade = self.assign_grade(percentage)
            yield student_info['name'], grade

    def assign_grade(self, percentage):
        if percentage < 40:
            return 'Fail'
        elif 40 <= percentage < 60:
            return 'D'
        elif 60 <= percentage < 70:
            return 'C'
        elif 70 <= percentage < 80:
            return 'B'
        elif 80 <= percentage < 90:
            return 'A'
        else:
            return 'A+'


if __name__ == "__main__":
    CalculateGrades.run()


"""
ren grades.py grades.py
python grades.py marks.csv

"""