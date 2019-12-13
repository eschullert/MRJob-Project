from mrjob.job import MRJob

class MR_Average(MRJob):
    '''
    Promedio de los ratings de todas las peliculas.
    '''
    def mapper(self,_, line):
        peli,usr,rating,fecha = line.split(',')
        yield peli,int(rating)
        
    def reducer(self, key, values):
        s,c=0,0
        for i in values:
            s+=i
            c+=1
        yield key, s/c

if __name__=='__main__':
    MR_Average.run()
