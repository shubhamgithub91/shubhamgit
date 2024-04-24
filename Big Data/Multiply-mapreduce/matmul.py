from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MatrixMultiplicationJob(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        matrix_strs = line.split(' ')
        matrix1 = list(eval(matrix_strs[0]))
        matrix2 = list(eval(matrix_strs[1]))
        yield f'Product of {matrix1} and {matrix2}: ', [matrix1, matrix2]

    def reducer(self, key, values):
        values_list = list(values)
        result_matrix = self.matrix_multiply(
            values_list[0][0], values_list[0][1])
        
        
        
        yield key, result_matrix

    def matrix_multiply(self, matrix1, matrix2):
        result_matrix = []
        for i in range(len(matrix1)):
            row = []
            for j in range(len(matrix2[0])):
                sum_val = 0
                for k in range(len(matrix2)):
                    sum_val += matrix1[i][k] * matrix2[k][j]
                row.append(sum_val)
            result_matrix.append(row)
        return result_matrix


if __name__ == '__main__':
    MatrixMultiplicationJob.run()


"""
ren matmul.py matmul.py
python matmul.py mats.txt

"""