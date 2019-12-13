from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MR_Dates(MRJob):
    '''
    Saca la calificacion promedio en una fecha. Si se le da como atributo un periodo de tiempo (ejemplo: --period m, para ver hasta el mes) para agrupar por ese periodo.
    '''

    def configure_args(self):
        super(MR_Dates, self).configure_args()
        self.add_passthru_arg(
            '--period', choices=['y','m','d'], default='y',
            help="Specify the movie you want to see dates for")
    
    def steps(self):
        return [
                MRStep(mapper = self.mapper_input,
                    reducer = self.reducer_average)
                ]

    def mapper_input(self,_, line):
        peli,usr,rating,fecha = line.split(',')
        peli,usr,rating = int(peli),int(usr),int(rating)
        if self.options.period == 'y': fecha = fecha[:4]
        elif self.options.period == 'm': fecha= fecha[:7]
        yield fecha,rating
        
    def reducer_average(self, key, values):
        s,c=0,0
        for i in values:
            s+=i
            c+=1
        yield key, s/c

if __name__=='__main__':
    MR_Dates.run()
