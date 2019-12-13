from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MR_Average(MRJob):
    '''
    Calificacion promedio de los usuarios
    '''
    def steps(self):
        return [
                MRStep(mapper = self.mapper_input,
                    reducer = self.reducer_average)
                ]

    def mapper_input(self,_, line):
        peli,usr,rating,fecha = line.split(',')
        peli,usr,rating,fecha = int(peli),int(usr),int(rating),datetime.strptime(fecha,'%Y-%m-%d')
        yield usr,rating
        
    def reducer_average(self, key, values):
        s,c=0,0
        for i in values:
            s+=i
            c+=1
        yield key, s/c        

if __name__=='__main__':
    MR_Average.run()
