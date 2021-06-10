from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class time_analysis_avg_value(MRJob):

    def mapper(self, _, line):

        try:
            fields = line.split(',')
            timestamp = int(fields[6]) #Month and year
            date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m')
            value = int(fields[3])

            yield(date,value)

        except:
            pass



    def reducer(self, date, values):
        iterator = list(values)
        average = int(sum(iterator)/len(iterator))
        yield(date, average)



if __name__ == '__main__':
    time_analysis_avg_value.run()
    # print("\n------------ %s seconds -----------" % np.round((time.time() - start_time),3))

    # time_analysis.JOBCONF= { 'mapreduce.job.reduces': '1' }
    # time_analysis.JOBCONF = {"mapred.textoutputformat.separator", ","}
    # conf.set("mapred.textoutputformat.separator", ",");
