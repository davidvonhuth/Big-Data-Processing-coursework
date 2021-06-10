from mrjob.job import MRJob
from mrjob.step import MRStep

class top_ten_miners(MRJob):

    def mapper1(self, _, line):

        try:
            fields = line.split(',')
            miner = fields[2]
            size = int(fields[4])
            yield(miner, size)

        except:
            pass



    def reducer1(self, miner, sizes):
        yield(None, (miner,sum(sizes)))

    def reducer2(self, _, values):
        sorted_values = sorted(values, reverse=True, key = lambda x: x[1])
        top_ten = sorted_values[0:10]

        for miner in top_ten:
            yield("{} - {}".format(miner[0], miner[1]), None)

    def steps(self):
        return [MRStep(mapper=self.mapper1,
                        reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]




if __name__ == '__main__':
    top_ten_miners.run()
    # print("\n------------ %s seconds -----------" % np.round((time.time() - start_time),3))

    # time_analysis.JOBCONF= { 'mapreduce.job.reduces': '1' }
    # time_analysis.JOBCONF = {"mapred.textoutputformat.separator", ","}
    # conf.set("mapred.textoutputformat.separator", ",");
