from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MR_Dates(MRJob):
    '''
    Saca la calificacion promedio en una fecha. Si se le da como atributo la clave de una
    pelicula (ejemplo: --movie 1234) solamente checa las calificaciones de esa pelicula.
    '''

    def configure_args(self):
        super(MR_Dates, self).configure_args()
        self.add_passthru_arg(
            '--movie', type=int,
            help="Specify the movie you want to see dates for")
    
    def steps(self):
        return [
                MRStep(mapper = self.mapper_input,
                    reducer = self.reducer_average)
                ]

    def mapper_input(self,_, line):
        if not self.options.movie or line.startswith(self.options.movie+','):
            peli,usr,rating,fecha = line.split(',')
            peli,usr,rating = int(peli),int(usr),int(rating)
            yield fecha,rating
        else: pass
        
    def reducer_average(self, key, values):
        s,c=0,0
        for i in values:
            s+=i
            c+=1
        yield key, s/c

if __name__=='__main__':
    MR_Dates.run()
