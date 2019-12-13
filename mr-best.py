from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MR_Average(MRJob):
    '''
    Saca la pelicula mejor calificada
    '''
    def steps(self):
        return [
                MRStep(mapper = self.mapper_input,
                    reducer = self.reducer_average),
                MRStep(reducer = self.reducer_max_average)
                ]

    def mapper_input(self,_, line):
        peli,usr,rating,fecha = line.split(',')
        peli,usr,rating,fecha = int(peli),int(usr),int(rating),datetime.strptime(fecha,'%Y-%m-%d')
        yield peli,rating
        
    def reducer_average(self, key, values):
        s,c=0,0
        for i in values:
            s+=i
            c+=1
        yield None, (s/c,key)

    def reducer_max_average(self, _, values):
        yield max(values)
        

if __name__=='__main__':
    MR_Average.run()
