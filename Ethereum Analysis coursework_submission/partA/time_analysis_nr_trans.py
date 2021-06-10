from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class time_analysis_nr_trans(MRJob):

    def mapper(self, _, line):

        try:
            fields = line.split(',')
            timestamp = int(fields[6]) #Month and year
            date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m')

            yield(date,1)

        except:
            pass



    def reducer(self, date, values):
        yield(date, sum(values))




if __name__ == '__main__':
    time_analysis_nr_trans.run()
    # print("\n------------ %s seconds -----------" % np.round((time.time() - start_time),3))

    # time_analysis.JOBCONF= { 'mapreduce.job.reduces': '1' }
    # time_analysis.JOBCONF = {"mapred.textoutputformat.separator", ","}
    # conf.set("mapred.textoutputformat.separator", ",");
